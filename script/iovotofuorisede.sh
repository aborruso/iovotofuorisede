#!/bin/bash

set -x
set -e
set -u
set -o pipefail

folder="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

mkdir -p "$folder"/../dati/rawdata
mkdir -p "$folder"/../dati/processing

# scarica dati, se non esistono già
if [ ! -f "$folder"/../dati/rawdata/iovotofuorisede_wide.csv ]; then
  curl -skL "https://docs.google.com/spreadsheets/d/e/2PACX-1vR3iLXlnpB6gbIl-6UxvStvYMl9am6WhLVcVqRiJx9XL_9C6QS6eEgJzEnkrOeRrcK8sylhF1gG-0MX/pub?gid=1958828636&single=true&output=csv" >"$folder"/../dati/rawdata/iovotofuorisede_wide.csv
  mlr -I --csv clean-whitespace "$folder"/../dati/rawdata/iovotofuorisede_wide.csv
fi

# crea versione long
mlr <"$folder"/../dati/rawdata/iovotofuorisede_wide.csv --csv reshape -r "^[^Reg]" -o i,v then \
  label origine,destinazione,valore then \
  put 'if($origine==$destinazione){$check=1}else{$check=0}' >"$folder"/../dati/rawdata/iovotofuorisede_long.csv

# normalizza nomi regione
mlr -I --csv put -S '$origine=sub($origine,"Trentino A.A.","Trentino-Alto Adige");$origine=sub($origine,"Fiuli V.G.","Friuli Venezia Giulia");$origine=sub($origine,"Emilia Romagna","Emilia-Romagna")' then \
put -S '$destinazione=sub($destinazione,"Trentino A.A.","Trentino-Alto Adige");$destinazione=sub($destinazione,"Fiuli V.G.","Friuli Venezia Giulia");$destinazione=sub($destinazione,"Emilia Romagna","Emilia-Romagna")' "$folder"/../dati/rawdata/iovotofuorisede_long.csv


# genera file CSV con centroidi in coordinate geografiche delle regioni italiane
if [ ! -f "$folder"/../dati/risorse/Reg01012021_g_WGS84.csv ]; then
  nomeFile=$(find "$folder"/.. -name "Reg*.shp")
  mapshaper "$nomeFile" -proj wgs84 -each 'cx=this.innerX, cy=this.innerY' -points x=cx y=cy -o "$folder"/../dati/risorse/Reg01012021_g_WGS84.csv
  mlr -I --csv cut -x -f COD_RIP,Shape_Leng,Shape_Area "$folder"/../dati/risorse/Reg01012021_g_WGS84.csv
fi

# aggiungi codice regione origine
mlr --csv join --ul -j "origine" -l "origine" -r "DEN_REG" -f "$folder"/../dati/rawdata/iovotofuorisede_long.csv then unsparsify then cut -x -f cx,cy then rename COD_REG,COD_REG_o "$folder"/../dati/risorse/Reg01012021_g_WGS84.csv > "$folder"/../dati/processing/iovotofuorisede.csv

# aggiungi codice regione destinazione
mlr --csv join --ul -j "destinazione" -l "destinazione" -r "DEN_REG" -f "$folder"/../dati/processing/iovotofuorisede.csv then unsparsify then cut -x -f cx,cy then rename COD_REG,COD_REG_d then reorder -f origine then sort -n COD_REG_o,COD_REG_d "$folder"/../dati/risorse/Reg01012021_g_WGS84.csv | sponge "$folder"/../dati/processing/iovotofuorisede.csv

# crea file popolazione con nomi regioni normalizzati rispetto a file geografico ISTAT
mlr --csv put '$Regione=sub($Regione,"Valle d'\''Aosta/Vallée d'\''Aoste","Valle d'\''Aosta");$Regione=sub($Regione,"Trentino-Alto Adige/Südtirol","Trentino-Alto Adige");$Regione=sub($Regione,"Friuli-Venezia Giulia","Friuli Venezia Giulia");' "$folder"/../dati/risorse/regioni.csv >"$folder"/../dati/processing/tmp_regioni.csv

# crea file di anagrafica in cui per ogni regione ci sono coordinate centroide e popolazione totale
mlr --csv join --ul -j "DEN_REG" -l "DEN_REG" -r "Regione" -f "$folder"/../dati/risorse/Reg01012021_g_WGS84.csv then unsparsify then cut -x -f Età then rename "Totale,Popolazione" then cut -x -r -f "Tota" "$folder"/../dati/processing/tmp_regioni.csv >"$folder"/../dati/processing/anagraficaRegioni.csv

# aggiungi dati popolazione e crea file di anagrafica regionale
mlr --csv join --ul -j "COD_REG_o" -l "COD_REG_o" -r "COD_REG" -f "$folder"/../dati/processing/iovotofuorisede.csv then unsparsify "$folder"/../dati/processing/anagraficaRegioni.csv

# aggiungi a dati iovotofuorisede coordinate centroide e popolazione
mlr --csv join --ul -j "COD_REG_o" -l "COD_REG_o" -r "COD_REG" -f "$folder"/../dati/processing/iovotofuorisede.csv then unsparsify then cut -x -f DEN_REG then reorder -f COD_REG_o,COD_REG_d "$folder"/../dati/processing/anagraficaRegioni.csv | sponge "$folder"/../dati/processing/iovotofuorisede.csv

# aggiungi calcolo per ogni 100.000 abitanti
mlr -I --csv put '$ogniCentomila=$valore/$Popolazione*100000' "$folder"/../dati/processing/iovotofuorisede.csv


### flowmap ###

# estrai locations
mlr --csv cut -o -f COD_REG,DEN_REG,cx,cy then label id,name,lon,lat then reorder -f id,name,lat,lon "$folder"/../dati/processing/anagraficaRegioni.csv >"$folder"/../dati/processing/locations.csv

# estrai flows Sicilia
mlr --csv filter -S '$COD_REG_o=="19" && $check=="0"' then cut -o -f COD_REG_o,COD_REG_d,ogniCentomila then label origin,dest,count "$folder"/../dati/processing/iovotofuorisede.csv >"$folder"/../dati/processing/flows_19.csv

# estrai flows
mlr --csv filter -S '$check=="0"' then cut -o -f COD_REG_o,COD_REG_d,ogniCentomila then label origin,dest,count "$folder"/../dati/processing/iovotofuorisede.csv >"$folder"/../dati/processing/flows.csv
