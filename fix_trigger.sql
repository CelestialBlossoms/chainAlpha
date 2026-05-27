-- Fix: Add missing review_ts and review_hour_ts columns to the trigger function
-- that inserts into bottom_top100_push_kline_reviews.
-- The function was missing these NOT NULL columns, causing every INSERT into
-- bottom_top100_push_records to fail silently.

CREATE OR REPLACE FUNCTION generate_bottom_strategy_trigger()
RETURNS TRIGGER AS $$
    DECLARE
        mcap NUMERIC;
        ath_mcap NUMERIC;
        liq NUMERIC;
        pool_ratio NUMERIC;
        age_sec BIGINT;
        price_change NUMERIC;
        signal_type TEXT;
        ath_ratio NUMERIC;

        score INTEGER := 60;
        risk_score INTEGER := 30;
        risk_tags TEXT[] := ARRAY[]::TEXT[];
        position_size INTEGER := 100;

        template TEXT := 'NONE';
        action TEXT := 'WATCH';
        skip_reason TEXT := NULL;

        suggested_tp JSONB;
        suggested_sl_pct NUMERIC := -15;
        hold_limit INTEGER := 120;
        strategy_json JSONB;

        now_epoch BIGINT;
    BEGIN
        now_epoch := EXTRACT(EPOCH FROM now())::BIGINT;

        mcap := NEW.current_mcap;
        ath_mcap := NEW.ath_mcap;
        liq := NEW.liquidity;
        pool_ratio := NEW.pool_mcap_ratio;
        age_sec := NEW.age_sec;
        price_change := NEW.price_change_pct;
        signal_type := NEW.signal_type;

        IF ath_mcap > 0 THEN
            ath_ratio := mcap / ath_mcap;
        ELSE
            ath_ratio := 1.0;
        END IF;

        -- 1. Hard Risk Filters
        IF price_change > 200 THEN
            action := 'SKIP';
            skip_reason := 'chase_trap';
            risk_tags := array_append(risk_tags, 'chase_trap');
            risk_score := 95;
        ELSIF liq < 10000 THEN
            action := 'SKIP';
            skip_reason := 'low_liquidity';
            risk_tags := array_append(risk_tags, 'low_liquidity');
            risk_score := 90;
        ELSIF signal_type IN ('drop_40w', 'drop_50w') THEN
            action := 'SKIP';
            skip_reason := 'decay_signal';
            risk_tags := array_append(risk_tags, 'decay_signal');
            risk_score := 85;
        ELSIF price_change < 15 THEN
            action := 'SKIP';
            skip_reason := 'low_momentum';
            risk_tags := array_append(risk_tags, 'low_momentum');
            risk_score := 75;
        END IF;

        IF action = 'SKIP' THEN
            INSERT INTO bottom_top100_push_kline_reviews (
                push_record_id, chain, source, address, symbol, signal_type, event_ts,
                current_mcap, first_signal_mcap, price_change_pct, liquidity,
                resolution, kline_from_ts, kline_to_ts, valid, invalid_reason,
                strategy_template, strategy_verdict, strategy_json, risk_tags, risk_score,
                review_ts, review_hour_ts
            ) VALUES (
                NEW.id, NEW.chain, NEW.source, NEW.address, NEW.symbol, NEW.signal_type, NEW.event_ts,
                mcap, NEW.first_signal_mcap, price_change, liq,
                '5m', NEW.event_ts, NEW.event_ts, FALSE, 'FILTERED: ' || skip_reason,
                'FILTERED', 'filtered', jsonb_build_object('skip_reason', skip_reason), risk_tags, risk_score,
                now_epoch, (now_epoch / 3600)::BIGINT
            ) ON CONFLICT (push_record_id) DO NOTHING;

            RETURN NEW;
        END IF;

        -- 2. Scoring
        IF liq >= 60000 AND liq <= 100000 THEN
            score := score + 15;
        ELSIF liq > 100000 THEN
            score := score + 10;
        ELSIF liq < 30000 THEN
            score := score - 10;
            risk_tags := array_append(risk_tags, 'thin_liquidity');
        END IF;

        IF ath_ratio < 0.05 THEN
            score := score + 15;
        ELSIF ath_ratio >= 0.05 AND ath_ratio <= 0.10 THEN
            score := score - 10;
            risk_tags := array_append(risk_tags, 'mid_level_trap');
        END IF;

        IF age_sec >= 86400 AND age_sec <= 172800 THEN
            score := score + 10;
        ELSIF age_sec > 30 * 86400 THEN
            score := score + 10;
        ELSIF age_sec < 3600 THEN
            risk_tags := array_append(risk_tags, 'extreme_new');
        END IF;

        IF pool_ratio > 0.5 THEN
            risk_tags := array_append(risk_tags, 'high_pool_mcap_ratio');
        END IF;

        IF age_sec < 86400 AND liq < 30000 THEN
            position_size := 50;
        ELSIF ath_ratio >= 0.05 AND ath_ratio <= 0.10 THEN
            position_size := 70;
        ELSIF age_sec >= 7 * 86400 AND age_sec <= 30 * 86400 THEN
            position_size := 70;
        ELSIF pool_ratio > 0.5 THEN
            position_size := 70;
        END IF;

        IF 'thin_liquidity' = ANY(risk_tags) THEN risk_score := risk_score + 20; END IF;
        IF 'mid_level_trap' = ANY(risk_tags) THEN risk_score := risk_score + 15; END IF;
        IF 'extreme_new' = ANY(risk_tags) THEN risk_score := risk_score + 25; END IF;
        IF 'high_pool_mcap_ratio' = ANY(risk_tags) THEN risk_score := risk_score + 10; END IF;
        IF risk_score > 100 THEN risk_score := 100; END IF;

        -- 3. Template Matching
        IF signal_type = 'new_revival' AND age_sec >= 86400 AND age_sec <= 172800 AND mcap < 200000 AND liq >= 30000 AND price_change >= 15 AND price_change <= 100 THEN
            template := 'C';
            suggested_tp := '{"targets": [30, 80], "sizes": [50, 50]}'::jsonb;
            suggested_sl_pct := -20;
            hold_limit := 90;
        ELSIF signal_type IN ('abnormal', 'quiet_runup') AND age_sec > 30 * 86400 AND mcap >= 100000 AND mcap <= 500000 AND liq >= 60000 THEN
            template := 'B';
            suggested_tp := '{"targets": [25, 50, 80], "sizes": [40, 30, 30]}'::jsonb;
            suggested_sl_pct := -15;
            hold_limit := 240;
        ELSIF signal_type IN ('abnormal', 'new_revival') AND mcap >= 50000 AND mcap <= 200000 AND liq >= 30000 AND ath_ratio < 0.1 THEN
            template := 'A';
            suggested_tp := '{"targets": [30, 60, 100], "sizes": [50, 30, 20]}'::jsonb;
            suggested_sl_pct := -22;
            hold_limit := 120;
        ELSIF signal_type = 'quiet_runup' AND mcap > 500000 AND liq >= 60000 THEN
            template := 'D';
            suggested_tp := '{"targets": [20, 40], "sizes": [30, 30], "trailing_stop": 40}'::jsonb;
            suggested_sl_pct := -12;
            hold_limit := 210;
        ELSIF price_change >= 60 AND price_change <= 100 AND mcap >= 50000 AND mcap <= 300000 AND liq >= 30000 THEN
            template := 'E';
            suggested_tp := '{"targets": [30, 70], "sizes": [50, 50]}'::jsonb;
            suggested_sl_pct := -18;
            hold_limit := 180;
        END IF;

        IF template <> 'NONE' AND score >= 65 THEN
            action := 'EXECUTE';
        ELSE
            action := 'WATCH';
        END IF;

        IF suggested_tp IS NULL THEN
            suggested_tp := '{"targets": [20, 50], "sizes": [50, 50]}'::jsonb;
        END IF;

        strategy_json := jsonb_build_object(
            'action', action,
            'strategy_name', CASE WHEN template <> 'NONE' THEN 'Template ' || template ELSE 'WATCH' END,
            'take_profit', suggested_tp,
            'stop_loss_pct', suggested_sl_pct,
            'hold_limit_min', hold_limit,
            'position_size_pct', position_size,
            'features', jsonb_build_object(
                'mcap', mcap,
                'athMcap', ath_mcap,
                'liquidity', liq,
                'ageSec', age_sec,
                'priceChangePct', price_change,
                'athRatio', round(ath_ratio::numeric, 4),
                'poolMcapRatio', pool_ratio
            )
        );

        INSERT INTO bottom_top100_push_kline_reviews (
            push_record_id, chain, source, address, symbol, signal_type, event_ts,
            current_mcap, first_signal_mcap, price_change_pct, liquidity,
            resolution, kline_from_ts, kline_to_ts, valid,
            strategy_template, strategy_verdict, strategy_json, risk_tags, risk_score,
            review_ts, review_hour_ts
        ) VALUES (
            NEW.id, NEW.chain, NEW.source, NEW.address, NEW.symbol, NEW.signal_type, NEW.event_ts,
            mcap, NEW.first_signal_mcap, price_change, liq,
            '5m', NEW.event_ts, NEW.event_ts + (hold_limit * 60), TRUE,
            template, CASE WHEN action = 'EXECUTE' THEN 'pending' ELSE 'watch' END, strategy_json, risk_tags, risk_score,
            now_epoch, (now_epoch / 3600)::BIGINT
        ) ON CONFLICT (push_record_id) DO NOTHING;

        RETURN NEW;
    END;
$$ LANGUAGE plpgsql;
