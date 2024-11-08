# EWE - Charging data

Python skript **_save_charging_data.py_** čeká na připojení/odpojení automobilu a následně zpracuje aktuální nabíjecí data, uloží je do CSV souboru a případně odešle do EMM aplikace.

## Instalace skriptu

Pro nainstalování skriptu je potřeba nahrát tyto soubory na kontroler do složky **_/data/user-app/charging_data_**
1. **_save_charging_data.py_** - skript s hlavní logikou pro sbírání, ukládání a odesílaní nabíjecích dat
2. **_collect_data_json.py_** - skript, který získá data kontroleru z API a uloží je do JSON souboru
3. **_utils.py_** - pomocné funkce, které jsou ve skriptu použity
4. **_charging_data.conf_** - konfigurační soubor

### Nastavení automatického spouštění skriptu při startu kontroleru

Aby se skript automaticky spustil, je potřeba upravit soubor **_data/user-app/user-application-start_** a vložit nový příkaz, který spustí skript při startu kontroleru.

```
# Save charging data to CSV file and send them to EMM if configured
/usr/bin/python3 /data/user-app/charging_data/save_charging_data.py &
```

### Spuštění skriptu bez nutnosti restartu

```
nohup /usr/bin/python3 /data/user-app/charging_data/save_charging_data.py &
```

## Skript pro vytvoření JSON souboru s daty kontroleru

Pro stahování CSV souboru s nabíjecími daty nahráváme na kontrolery také vlastní webovou stránku.
Na webové stránce zobrazujeme také obecné informace o nabíjecích kontrolerech. Vzhledem k tomu, že webová stránka je statická a nemá žádný backend, museli bychom data o kontrolerech získat z API požadavkem přímo z klienta. Z toho důvodu jsme vytvořili Python skript, který každých 5 sekund získá data z API a uloží je do JSON souboru, který se nachází přímo v adresáři webové stránky.

- **_collect_data_json.py_**

Stejně jako u předešlého skriptu je potřeba upravit soubor **_data/user-app/user-application-start_**, aby se skript automaticky spustil při startu kontroleru.

```
# Collect charging controller data to JSON
/usr/bin/python3 /data/user-app/charging_data/collect_data_json.py &
```
