#!/bin/bash

# 1. Pega a data no formato AAAA-MM-DD
data=$(date +%Y-%m-%d)

# 2. Define onde está o projeto e onde o backup será salvo
DIR_ORIGEM="/caminho/absoluto/para/Ro-dou"
DIR_BACKUP="$DIR_ORIGEM/backup"

# Cria a pasta de backup caso ela ainda não exista
mkdir -p "$DIR_BACKUP"

# 3. Nome do arquivo final
ARQUIVO_DESTINO="$DIR_BACKUP/backup-rodou-$data.tar.gz"

# 4. Executa o backup compactado (ignorando a própria pasta de backup)
tar -czvf "$ARQUIVO_DESTINO" --exclude="$DIR_BACKUP" "$DIR_ORIGEM"

echo "Backup concluído: $ARQUIVO_DESTINO"