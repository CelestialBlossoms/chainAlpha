# Solana CA Holders

Fetch Top holders for a Solana SPL token mint/CA through `getProgramAccounts`.

```powershell
D:/software/anaconda/envs/devpy/python.exe solana_ca_holders/get_sol_top_holders.py DPZcf7hDSVhDVSC8KLU17p7bSCCWr8htwM9Y2aWDnwez --limit 500
```

Save files:

```powershell
D:/software/anaconda/envs/devpy/python.exe solana_ca_holders/get_sol_top_holders.py <CA> --limit 500 --json-out solana_ca_holders/output/top500.json --csv-out solana_ca_holders/output/top500.csv
```

If the public Solana RPC rejects `getProgramAccounts`, use a paid/private RPC:

```powershell
D:/software/anaconda/envs/devpy/python.exe solana_ca_holders/get_sol_top_holders.py <CA> --rpc https://your-rpc.example --limit 500
```
