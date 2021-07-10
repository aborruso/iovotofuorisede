#!/bin/bash

set -x
set -e
set -u
set -o pipefail

folder="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

mkdir -p "$folder"/../dati/risorse

URL_limiti="https://www.istat.it/storage/cartografia/confini_amministrativi/generalizzati/Limiti01012021_g.zip"

# se la cartella con i confini regionali non esiste
if [[ -z $(find "$folder"/.. -name "Reg*") ]]; then
  # scarica dati
  wget -O "$folder"/../dati/risorse/Limiti01012021_g.zip "$URL_limiti"
  # decomprimi file
  unzip "$folder"/../dati/risorse/Limiti01012021_g.zip -d "$folder"/../dati/risorse
  # cancella file scaricato
  rm "$folder"/../dati/risorse/Limiti01012021_g.zip
  # cancella tutte le cartelle tranne quella con dati regionali
  find "$folder"/../dati/risorse/Limiti01012021_g/ -mindepth 1 -type d ! -name "Reg*_g" -exec rm -rf {} +
fi

URL_popolazione="http://demo.istat.it/pop2021/dati/regioni.zip"

# se i dati sulla popolazione non esistono, scaricali
if [ ! -f "$folder"/../dati/risorse/regioni.csv ]; then
  wget -O "$folder"/../dati/risorse/regioni.zip "$URL_popolazione"
  unzip "$folder"/../dati/risorse/regioni.zip -d "$folder"/../dati/risorse
  rm "$folder"/../dati/risorse/regioni.zip
  tail <"$folder"/../dati/risorse/regioni.csv -n +2 | mlr --csv filter -S '${Età}=="Totale"' then put '$Totale=${Totale Maschi}+${Totale Femmine}' | sponge "$folder"/../dati/risorse/regioni.csv
fi
