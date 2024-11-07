# EWE - Charging data

Python skript **_ewe_charging_data_native_csv.py_** čeká na připojení/odpojení automobilu a následně zpracuje
aktuální data a uloží je do CSV souboru.

## Instalace skriptu

Pro nainstalování skriptu je potřeba nahrát tyto soubory na kontroler do složky **_/data/user-app/charging_data_**
1. **_ewe_charging_data_native_csv.py_** - skript s hlavní logikou sbírání nabíjecích dat
2. **_utils.py_** - pomocné funkce, které jsou ve skriptu použity
3. **_charging_data.conf_** - konfigurační soubor

### Nastavení automatického spouštění skriptu při startu kontroleru

Aby se skript automaticky spustil, je potřeba upravit soubor **_data/user-app/user-application-start_** a vložit nový příkaz, který spustí skript při startu kontroleru.

```
# Charging data counter script
/usr/bin/python3 /data/user-app/charging_data/ewe_charging_data_native_csv.py &
```

## Skript pro vytvoření JSON souboru s daty kontroleru

Pro stahování CSV souboru s nabíjecími daty nahráváme na kontrolery také vlastní webovou stránku.
Na webové stránce zobrazujeme také obecné informace o nabíjecích kontrolerech. Vzhledem k tomu, že webová stránka je statická a nemá žádný backend, museli bychom data o kontrolerech získat z API požadavkem přímo z klienta. Z toho důvodu jsme vytvořili Python skript, který každých 5 sekund získá data z API a uloží je do JSON souboru, který se nachází přímo v adresáři webové stránky.

- **_collect_data_json.py_** - skript, který získá data kontroleru z API a uloží je do JSON souboru

Stejně jako u předešlého skriptu je potřeba upravit soubor **_data/user-app/user-application-start_**, aby se skript automaticky spustil při startu kontroleru.

```
# Collect charging controller data to JSON
/usr/bin/python3 /data/user-app/charging_data/collect_data_json.py &
```
