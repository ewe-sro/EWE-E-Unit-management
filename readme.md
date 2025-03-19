# EWE - Charging data

Python skripty v tom repozitáři se primárně starají za propojení nabíjecí stanice s webovou aplikací EMM.

## Popis souborů
1. **_save_charging_data.py_** - skript s hlavní logikou pro sbírání, ukládání a odesílaní nabíjecích relací
2. **_collect_data_json.py_** - skript, který získá data kontroleru z API a uloží je do JSON souboru a případně odešle do EMM
3. **_sync_settings.py_** - skript, který synchronizuje nastavení z EMM webové aplikace s interním nastavením nabíjecích bodů
4. **_update.py_** - skript, který aktualizuje skripty z tohoto repozitáře na nějnovější verzi
5. **_utils.py_** - pomocné funkce, které jsou ve skriptech použity
6. **_charging_data_example.conf_** - ukázka konfiguračního souboru - je potřeba vyplnit a přejmenovat na _charging_data.conf_

## Instalace skriptů pomocí **_update.py_**

K instalaci skriptů je možné použít skript **_update.py_**. Je potřeba nakopírovat tyto soubory na kontroler do složky **_/data/user-app/charging_data_**:
1. **_update.py_**
2. **_utils.py_**
3. **_charging_data_example.conf_** - je potřeba vyplnit a přejmenovat na _charging_data.conf_

A poté spustit skript **_update.py_** pomocí tohoto příkazu
```
/usr/bin/python3 /data/user-app/charging_data/update.py
```

## Manuální instalace skriptů

Pro nainstalování skriptu je potřeba nahrát všechny soubory na kontroler do složky **_/data/user-app/charging_data_**.

## Skript pro ukládání nabíjecích relaci

Python skript **_save_charging_data.py_** čeká na připojení/odpojení automobilu a následně zpracuje aktuální nabíjecí data, uloží je do CSV souboru a případně odešle do EMM aplikace (pokud je nastaven v kofiguračním souboru API klíč).

### Nastavení automatického spouštění skriptu při startu kontroleru

Aby se skript automaticky spustil, je potřeba upravit soubor **_data/user-app/user-application-start_** a vložit nový příkaz, který spustí skript při startu kontroleru.

```
# Save charging data to CSV file and send them to EMM if configured
/usr/bin/python3 /data/user-app/charging_data/save_charging_data.py &
```

## Skript pro vytvoření JSON souboru s daty kontroleru

- **_collect_data_json.py_**

Pro stahování CSV souboru s nabíjecími daty nahráváme na kontrolery také vlastní webovou stránku.
Na webové stránce zobrazujeme také obecné informace o nabíjecích kontrolerech. Vzhledem k tomu, že webová stránka je statická a nemá žádný backend, museli bychom data o kontrolerech získat z API požadavkem přímo z klienta. Z toho důvodu jsme vytvořili Python skript, který každých 5 sekund získá data z API a uloží je do JSON souboru, který se nachází přímo v adresáři webové stránky. Tento skript také odesílá data do webové aplikace EMM, pokud je v konfiguračním souboru nastaven API klíč.

Stejně jako u předešlého skriptu je potřeba upravit soubor **_data/user-app/user-application-start_**, aby se skript automaticky spustil při startu kontroleru.

```
# Collect charging controller data to JSON
/usr/bin/python3 /data/user-app/charging_data/collect_data_json.py &
```

## Skript pro synchronizaci nastavení s EMM

- **_save_charging_data.py_**

Abychom mohli změnit nastavení vzdáleně pomocí webové aplikace EMM běží na nabíjecí stanici tento skript. Skript nejdříve získá nastavení z EMM, pokud nějaká existují, tato nastavení aplikuje.
Následně získá aktuální nastavení nabíjecího bodu a tato nastavení odešle do EMM k uložení. Tento způsob nám dovoluje upravit nastavení jak z webové aplikace, tak z lokální administrace nabíjecí stanice a zároveň zůstanou data o nastavení v EMM konzistentní.

```
# Sync controller settings with EMM database
/usr/bin/python3 /data/user-app/charging_data/sync_settings.py &
```

### Manuální spuštění skriptů bez nutnosti restartu

```
nohup /usr/bin/python3 /data/user-app/charging_data/save_charging_data.py &
nohup /usr/bin/python3 /data/user-app/charging_data/collect_data_json.py &
nohup /usr/bin/python3 /data/user-app/charging_data/sync_settings.py &
```
