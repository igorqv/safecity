# ──────────────────────────────────────────────────────────────────────────────
# Makefile — SafeCity SP — Atalhos para operações Docker + desenvolvimento
#
# Uso:
#   make help          # Ver todos os comandos
#   make build         # Build da imagem Docker
#   make run           # Executar pipeline completo
#   make logs          # Ver logs do container
#   make clean         # Remover containers + volumes
# ──────────────────────────────────────────────────────────────────────────────

.PHONY: help build run run-detached logs logs-follow shell ingest clean compute load down clean-volumes

# Configurações padrão
DOCKER_IMAGE := safecity-pipeline
DOCKER_COMPOSE := docker-compose

help:
	@echo "SafeCity SP — Atalhos Docker"
	@echo ""
	@echo "Setup:"
	@echo "  make setup              Copiar .env.example → .env e criar raw/"
	@echo ""
	@echo "Build & Execução:"
	@echo "  make build              Build da imagem Docker"
	@echo "  make run                Executar pipeline completo (bloqueante)"
	@echo "  make run-detached       Executar pipeline em background"
	@echo ""
	@echo "Etapas individuais:"
	@echo "  make ingest             Rodar só ingest.py"
	@echo "  make clean              Rodar só clean.py"
	@echo "  make compute            Rodar só compute_kpis.py"
	@echo "  make load               Rodar só load_supabase.py"
	@echo ""
	@echo "Debugging:"
	@echo "  make logs               Ver últimas linhas de log"
	@echo "  make logs-follow        Ver logs em tempo real (Ctrl+C para sair)"
	@echo "  make shell              Abrir shell bash no container"
	@echo ""
	@echo "Limpeza:"
	@echo "  make down               Parar containers"
	@echo "  make clean-volumes      Remover containers + volumes (CUIDADO)"

setup:
	@if [ ! -f .env ]; then \
		echo "Copiando .env.example → .env"; \
		cp .env.example .env; \
		echo "⚠️  Edite .env com suas credenciais Supabase"; \
	else \
		echo ".env já existe"; \
	fi
	@if [ ! -d raw ]; then \
		mkdir -p raw; \
		echo "Pasta raw/ criada. Adicione os arquivos .xlsx aqui."; \
	fi

build:
	$(DOCKER_COMPOSE) build

run:
	$(DOCKER_COMPOSE) up

run-detached:
	$(DOCKER_COMPOSE) up -d
	@echo "Pipeline iniciado em background."
	@echo "Ver logs com: make logs-follow"

ingest:
	$(DOCKER_COMPOSE) run $(DOCKER_IMAGE) python ingest.py

clean:
	$(DOCKER_COMPOSE) run $(DOCKER_IMAGE) python clean.py

compute:
	$(DOCKER_COMPOSE) run $(DOCKER_IMAGE) python compute_kpis.py

load:
	$(DOCKER_COMPOSE) run $(DOCKER_IMAGE) python load_supabase.py

logs:
	$(DOCKER_COMPOSE) logs --tail 50

logs-follow:
	$(DOCKER_COMPOSE) logs -f

shell:
	$(DOCKER_COMPOSE) run $(DOCKER_IMAGE) /bin/bash

down:
	$(DOCKER_COMPOSE) stop

clean-volumes:
	@echo "⚠️  Removendo containers + volumes (dados perdidos)..."
	@read -p "Tem certeza? [y/n] " -n 1 -r; \
	echo; \
	if [[ $$REPLY =~ ^[Yy]$$ ]]; then \
		$(DOCKER_COMPOSE) down -v; \
		echo "✓ Limpo"; \
	else \
		echo "Cancelado"; \
	fi

# Alias comuns
start: run-detached
stop: down
restart: down run-detached
rebuild: down clean-volumes build run
