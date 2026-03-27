# EWE E-Unit management

Python skripty v tom repozitáři se primárně starají o propojení nabíjecí stanice s webovou aplikací EMM.

## Aktivní skripty
1. **_ewe-charger-agenty.py_** - skript, který zpracovává MQTT události, spravuje SQLite frontu a odesílá telemetrii do EMM. Nahrazuje dřívější skripty `save_charging_data.py` a `collect_data_json.py`.
2. **_sync_settings.py_** - skript, který synchronizuje nastavení z EMM webové aplikace s interním nastavením nabíjecích bodů
3. **_update.py_** - skript, který aktualizuje skripty z tohoto repozitáře na nějnovější verzi
4. **_utils.py_** - pomocné funkce, které jsou ve skriptech použity
5. **_charging_data_example.conf_** - ukázka konfiguračního souboru - je potřeba vyplnit a přejmenovat na _charging_data.conf_

### Zastaralé soubory
> [!WARNING]
> Tyto skripty již nejsou aktivně vyvíjeny a jsou ponechány pouze pro archivní účely. Provoz na nových kontrolerech by měl využívat výhradně `ewe-charger-agent.py`.

1. **_save_charging_data.py_** - původní skript pro ukládání relací do CSV souboru
2. **_collect_data_json.py_** - původní skript pro sběr stavových dat do statického JSON souboru

## Instalace skriptů pomocí **_update.py_**

K instalaci skriptů je možné použít skript **_update.py_**. Je potřeba nakopírovat tyto soubory na kontroler do složky **_/data/user-app/charging_data_**:
1. **_update.py_**
2. **_charging_data_example.conf_** - je potřeba vyplnit a přejmenovat na `charging_data.conf`

A poté spustit skript **_update.py_** pomocí tohoto příkazu
```
/usr/bin/python3 /data/user-app/charging_data/update.py
```

### Nastavení automatického spouštění skriptu při startu kontroleru

Aby se skript automaticky spustil při startu kontroleru, je potřeba upravit soubor `data/user-app/user-application-start` a vložit nový příkaz, který spustí skript při startu kontroleru.
Použitím skriptu `update.py` se tato konfigurace provede automaticky.

```
# Save charging data to CSV file and send them to EMM if configured
/usr/bin/python3 /data/user-app/charging_data/ewe-charger-agenty.py &
```

## Skript pro synchronizaci nastavení s EMM

- **_sync_settings.py_**

Abychom mohli změnit nastavení vzdáleně pomocí webové aplikace EMM běží na nabíjecí stanici tento skript. Skript nejdříve získá nastavení z EMM, pokud nějaká existují, tato nastavení aplikuje.
Následně získá aktuální nastavení nabíjecího bodu a tato nastavení odešle do EMM k uložení. Tento způsob nám dovoluje upravit nastavení jak z webové aplikace, tak z lokální administrace nabíjecí stanice a zároveň zůstanou data o nastavení v EMM konzistentní.

### Manuální spuštění skriptů bez nutnosti restartu

```
nohup /usr/bin/python3 /data/user-app/charging_data/ewe-charger-agenty.py &
nohup /usr/bin/python3 /data/user-app/charging_data/sync_settings.py &
```
