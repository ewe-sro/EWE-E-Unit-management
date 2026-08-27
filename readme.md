# EWE E-Unit management

Python skripty v tom repozitáři se primárně starají o propojení nabíjecí stanice s webovou aplikací EMM.

## Aktivní skripty
1. **_ewe-charger-agent.py_** - skript, který zpracovává MQTT události a nastavení, spravuje SQLite frontu a odesílá telemetrii do EMM. Nahrazuje všechny dřívější skripty.
2. **_update.py_** - skript, který aktualizuje skripty z tohoto repozitáře na nějnovější verzi
3. **_utils.py_** - pomocné funkce, které jsou ve skriptech použity
4. **_charging_data_example.conf_** - ukázka konfiguračního souboru - je potřeba vyplnit a přejmenovat na _charging_data.conf_

### Zastaralé soubory
> [!WARNING]
> Tyto skripty již nejsou aktivně vyvíjeny a jsou ponechány pouze pro archivní účely. Provoz na nových kontrolerech by měl využívat výhradně `ewe-charger-agent.py`.

1. **_save_charging_data.py_** - původní skript pro ukládání relací do CSV souboru
2. **_collect_data_json.py_** - původní skript pro sběr stavových dat do statického JSON souboru
3. **_sync_settings.py_** - původní skript pro synchronizaci nastavení z EMM webové aplikace s interním nastavením nabíjecích bodů

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
/usr/bin/python3 /data/user-app/charging_data/ewe-charger-agent.py &
```

### Manuální spuštění skriptů bez nutnosti restartu

```
nohup /usr/bin/python3 /data/user-app/charging_data/ewe-charger-agent.py &
```

### Lokální vývoj a testování (firmware 1.9.1+)

Od verze firmware CHARX OS **1.9.1** je externí přístup k REST API zabezpečen a interní REST API na portu `5555` naslouchá výhradně na lokálním rozhraní stanice (`127.0.0.1`).

Pro vývoj a testování skriptů přímo z vývojářského počítače (nebo při testování REST API v API klientech) je nejvhodnějším přístupem vytvoření **SSH tunelu**.

#### Vytvoření SSH tunelu
Spusťte ve vašem terminálu nebo PowerShellu tento příkaz, který přesměruje port `5555` z vašeho počítače přímo na interní rozhraní stanice:

```bash
ssh -L 5555:127.0.0.1:5555 -L 1883:127.0.0.1:1883 user-app@<ip-address>
