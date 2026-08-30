#!/usr/bin/env bash
set -euo pipefail

### Copyright (2022-2025) Variocube GmbH
### Variocube developer tools installation and management script

readonly VERSION="2.0.0"
readonly GITHUB_ZIP_URL="https://github.com/variocube/devtools/archive/refs/heads/main.zip"
readonly GITHUB_RAW_URL="https://raw.githubusercontent.com/variocube/devtools/main/devtools.sh"

# ─────────────────────────────────────────────────────────────────────────────
# Utility Functions
# ─────────────────────────────────────────────────────────────────────────────

# Detect color support
if [[ -t 1 ]] && command -v tput &>/dev/null && [[ $(tput colors 2>/dev/null || echo 0) -ge 8 ]]; then
	readonly RED=$(tput setaf 1)
	readonly GREEN=$(tput setaf 2)
	readonly YELLOW=$(tput setaf 3)
	readonly BLUE=$(tput setaf 4)
	readonly BOLD=$(tput bold)
	readonly RESET=$(tput sgr0)
else
	readonly RED="" GREEN="" YELLOW="" BLUE="" BOLD="" RESET=""
fi

die() {
	echo >&2 "${RED}Error:${RESET} $*"
	exit 1
}

info() {
	echo "${BLUE}→${RESET} $*"
}

success() {
	echo "${GREEN}✓${RESET} $*"
}

warn() {
	echo "${YELLOW}Warning:${RESET} $*"
}

confirm() {
	local response
	read -r -p "$1 [y/N] " response
	case "$response" in
		[yY][eE][sS]|[yY]) return 0 ;;
		*) return 1 ;;
	esac
}

prompt() {
	local var_name="$1"
	local prompt_text="$2"
	local default="${3:-}"
	local value

	if [[ -n "$default" ]]; then
		read -r -p "${prompt_text} [${default}]: " value
		value="${value:-$default}"
	else
		read -r -p "${prompt_text}: " value
	fi

	printf -v "$var_name" '%s' "$value"
}

prompt_password() {
	local var_name="$1"
	local prompt_text="$2"
	local value

	read -r -s -p "${prompt_text}: " value
	echo
	printf -v "$var_name" '%s' "$value"
}

require_cmd() {
	local cmd="$1"
	local install_hint="${2:-}"
	if ! command -v "$cmd" &>/dev/null; then
		if [[ -n "$install_hint" ]]; then
			die "'$cmd' is not installed. $install_hint"
		else
			die "'$cmd' is required but not installed."
		fi
	fi
}

# Cross-platform sed -i
sed_inplace() {
	if [[ "$OSTYPE" == "darwin"* ]]; then
		sed -i '' "$@"
	else
		sed -i "$@"
	fi
}

# ─────────────────────────────────────────────────────────────────────────────
# Git Functions
# ─────────────────────────────────────────────────────────────────────────────

is_git_repo() {
	git rev-parse --is-inside-work-tree &>/dev/null
}

get_devtools_files() {
	echo ".devtools"
	echo "devtools.sh"
	echo ".editorconfig"
	echo "dprint.json"
	echo ".idea/dprintProjectConfig.xml"
	echo ".idea/dprintUserConfig.xml"
	echo ".idea/eclipseCodeFormatter.xml"
	echo ".github/ISSUE_TEMPLATE.md"
	echo ".github/PULL_REQUEST_TEMPLATE.md"
}

commit_devtools() {
	if ! is_git_repo; then
		die "Not a git repository. Cannot commit."
	fi

	info "Staging devtools files..."
	local files
	files=$(get_devtools_files)
	local staged=0

	while IFS= read -r file; do
		if [[ -e "$file" ]] || [[ -L "$file" ]]; then
			git add "$file" 2>/dev/null && ((staged++)) || true
		fi
	done <<< "$files"

	if [[ "$staged" -eq 0 ]]; then
		warn "No devtools files found to stage."
		return 1
	fi

	# Check if there are staged changes
	if git diff --cached --quiet; then
		info "No changes to commit."
		return 1
	fi

	info "Committing..."
	git commit -m "chore: update devtools to v${VERSION}" || die "Failed to commit"
	success "Committed devtools v${VERSION}"
	return 0
}

push_devtools() {
	if ! is_git_repo; then
		die "Not a git repository. Cannot push."
	fi

	local branch
	branch=$(git rev-parse --abbrev-ref HEAD)

	info "Pushing to origin/${branch}..."
	git push origin "$branch" || die "Failed to push"
	success "Pushed to origin/${branch}"
}

# ─────────────────────────────────────────────────────────────────────────────
# Configuration Management
# ─────────────────────────────────────────────────────────────────────────────

WORK_DIR="$(pwd)"
CONFIG_FILE="${WORK_DIR}/.vc"
DEVTOOLS_DIR="${WORK_DIR}/.devtools"

# ─────────────────────────────────────────────────────────────────────────────
# Central registry (workspace projects.json) — preferred source of truth.
# Falls back silently to the per-repo .vc when the registry, jq, or a key is
# missing, so this works whether or not the repo sits under the workspace.
# ─────────────────────────────────────────────────────────────────────────────

# Locate projects.json by walking up from the working directory.
find_registry() {
	local dir="$WORK_DIR"
	while [[ -n "$dir" && "$dir" != "/" ]]; do
		if [[ -f "$dir/projects.json" ]]; then
			printf '%s' "$dir/projects.json"
			return 0
		fi
		dir=$(dirname "$dir")
	done
	return 1
}

# registry_get <jq-filter> [extra jq args...]
# Evaluates the filter with $p bound to the project (the repo directory name).
# Prints the resolved value, or nothing if unavailable.
registry_get() {
	local filter="$1"; shift
	local registry
	registry=$(find_registry) || return 0
	command -v jq &>/dev/null || return 0
	local project value
	project=$(basename "$WORK_DIR")
	value=$(jq -r --arg p "$project" "$@" "$filter" "$registry" 2>/dev/null) || return 0
	[[ "$value" == "null" ]] && value=""
	printf '%s' "$value"
}

# Load configuration if it exists
load_config() {
	if [[ -f "$CONFIG_FILE" ]]; then
		# shellcheck source=/dev/null
		source "$CONFIG_FILE"
	fi
}

# Save or update a config value
save_config_value() {
	local key="$1"
	local value="$2"

	if [[ ! -f "$CONFIG_FILE" ]]; then
		# Create new config file
		cat > "$CONFIG_FILE" <<-EOF
		# Variocube project configuration
		# Generated by devtools.sh v${VERSION}
		EOF
	fi

	if grep -q "^${key}=" "$CONFIG_FILE" 2>/dev/null; then
		# Update existing value
		sed_inplace "s|^${key}=.*|${key}=${value}|" "$CONFIG_FILE"
	else
		# Append new value
		echo "${key}=${value}" >> "$CONFIG_FILE"
	fi
}

# Ensure DATABASE_NAME is configured (prompt if missing)
ensure_database_config() {
	load_config
	if [[ -z "${DATABASE_NAME:-}" ]]; then
		info "Database name not configured."
		prompt DATABASE_NAME "Enter the database name"
		if [[ -z "$DATABASE_NAME" ]]; then
			die "Database name is required."
		fi
		save_config_value "DATABASE_NAME" "$DATABASE_NAME"
		success "Saved DATABASE_NAME to ${CONFIG_FILE}"
	fi
}

# Ensure AWS configuration is set
ensure_aws_config() {
	load_config

	# Precedence: explicit env AWS_PROFILE/REGION > registry (source of truth) > .vc > prompt.
	# The registry intentionally wins over .vc, which is deprecated and may be stale
	# (e.g. smalox-cloud's .vc wrongly names the variocube profile).
	local reg_profile reg_region
	reg_profile=$(registry_get '(.projects[$p].aws // .defaults.aws).profile')
	reg_region=$(registry_get '(.projects[$p].aws // .defaults.aws).region')

	# Check AWS_PROFILE
	if [[ -z "${AWS_PROFILE:-}" ]]; then
		local profile="${reg_profile:-${VC_AWS_PROFILE:-}}"
		if [[ -z "$profile" ]]; then
			info "AWS profile not configured."
			prompt profile "Enter the AWS profile name" "variocube"
			save_config_value "VC_AWS_PROFILE" "$profile"
			success "Saved VC_AWS_PROFILE to ${CONFIG_FILE}"
		fi
		export AWS_PROFILE="$profile"
	fi

	# Check AWS_REGION
	if [[ -z "${AWS_REGION:-}" ]]; then
		local region="${reg_region:-${VC_AWS_REGION:-}}"
		if [[ -z "$region" ]]; then
			info "AWS region not configured."
			prompt region "Enter the AWS region" "eu-west-1"
			save_config_value "VC_AWS_REGION" "$region"
			success "Saved VC_AWS_REGION to ${CONFIG_FILE}"
		fi
		export AWS_REGION="$region"
	fi
}

# Ensure CloudWatch log group is configured for a stage
ensure_logs_config() {
	local stage="${1:-app}"
	load_config
	ensure_aws_config

	local log_group_var="CLOUD_WATCH_LOG_GROUP_${stage}"

	# Registry wins over .vc (which load_config may have set into $log_group_var).
	local registry_log_group
	registry_log_group=$(registry_get '.projects[$p].stages[$stage].logGroup' --arg stage "$stage")
	if [[ -n "$registry_log_group" ]]; then
		declare -g "$log_group_var=$registry_log_group"
	fi

	# Fall back to .vc / interactive prompt only if still unset.
	if [[ -z "${!log_group_var:-}" ]]; then
		info "CloudWatch log group for stage '${stage}' not configured."
		prompt LOG_GROUP "Enter the CloudWatch log group name"
		if [[ -z "$LOG_GROUP" ]]; then
			die "Log group name is required."
		fi
		save_config_value "$log_group_var" "$LOG_GROUP"
		# Set the variable for immediate use
		declare -g "$log_group_var=$LOG_GROUP"
		success "Saved ${log_group_var} to ${CONFIG_FILE}"
	fi
}

# ─────────────────────────────────────────────────────────────────────────────
# MySQL Authentication
# ─────────────────────────────────────────────────────────────────────────────

MYSQL_SUDO=""
MYSQL_PASS=""

get_mysql_auth() {
	prompt_password MYSQL_PASS "Enter MySQL root password (leave empty for sudo on Linux)"

	# On Linux with empty password, use sudo
	if [[ "$OSTYPE" == "linux-gnu"* ]] && [[ -z "$MYSQL_PASS" ]]; then
		MYSQL_SUDO="sudo"
	else
		MYSQL_SUDO=""
	fi
}

run_mysql() {
	if [[ -n "$MYSQL_PASS" ]]; then
		$MYSQL_SUDO mysql -u root "-p${MYSQL_PASS}" "$@"
	else
		$MYSQL_SUDO mysql -u root "$@"
	fi
}

# ─────────────────────────────────────────────────────────────────────────────
# Core Installation
# ─────────────────────────────────────────────────────────────────────────────

install_devtools() {
	local target_dir="$1"
	local do_commit="${2:-0}"
	local do_push="${3:-0}"

	require_cmd wget
	require_cmd unzip

	local temp_zip
	temp_zip=$(mktemp)

	info "Downloading devtools..."
	wget -q "$GITHUB_ZIP_URL" -O "$temp_zip" || { rm -f "$temp_zip"; die "Failed to download devtools"; }

	info "Extracting..."
	unzip -q "$temp_zip" -d "$target_dir" || { rm -f "$temp_zip"; die "Failed to extract devtools"; }

	# Clean up temp file
	rm -f "$temp_zip"

	# Remove old devtools directory if it exists
	rm -rf "${target_dir}/.devtools"

	# Move extracted directory to .devtools
	mv "${target_dir}/devtools-main" "${target_dir}/.devtools"

	# Remove unnecessary files
	rm -rf "${target_dir}/.devtools/.idea"
	rm -rf "${target_dir}/.devtools/test"
	rm -f "${target_dir}/.devtools/CLAUDE.md"
	rm -f "${target_dir}/.devtools/.devtools"

	info "Creating symlinks..."

	# Create symlinks in target directory
	pushd "$target_dir" > /dev/null

	# Main symlinks
	ln -srf ".devtools/devtools.sh" "devtools.sh"
	ln -srf ".devtools/.editorconfig" ".editorconfig"
	ln -srf ".devtools/dprint.json" "dprint.json"

	# Coding guidelines and Claude context now live centrally in the workspace
	# (variocube/.claude/, auto-loaded for every repo). Remove the obsolete per-repo
	# symlink left by older devtools versions.
	rm -f ".claude/rules/devtools-guidelines.md"

	# IDEA settings
	mkdir -p ".idea"
	rm -rf ".idea/codeStyles"
	ln -srf ".devtools/idea/dprintProjectConfig.xml" ".idea/dprintProjectConfig.xml"
	ln -srf ".devtools/idea/dprintUserConfig.xml" ".idea/dprintUserConfig.xml"
	ln -srf ".devtools/idea/eclipseCodeFormatter.xml" ".idea/eclipseCodeFormatter.xml"

	# GitHub templates
	mkdir -p ".github"
	ln -srf ".devtools/ISSUE_TEMPLATE.md" ".github/ISSUE_TEMPLATE.md"
	ln -srf ".devtools/PULL_REQUEST_TEMPLATE.md" ".github/PULL_REQUEST_TEMPLATE.md"

	popd > /dev/null

	success "Devtools v${VERSION} installed successfully!"

	# Handle commit and push
	if [[ "$do_commit" == "1" ]]; then
		echo
		if commit_devtools; then
			if [[ "$do_push" == "1" ]]; then
				push_devtools
			fi
		fi
	else
		echo
		echo "Next steps:"
		echo "  1. Review and commit the .devtools directory and symlinks"
		echo "  2. Run ${BOLD}./devtools.sh help${RESET} to see available commands"
	fi
}

# ─────────────────────────────────────────────────────────────────────────────
# Database Commands
# ─────────────────────────────────────────────────────────────────────────────

cmd_db_create() {
	ensure_database_config
	get_mysql_auth

	info "Creating database '${DATABASE_NAME}'..."
	echo "CREATE DATABASE IF NOT EXISTS ${DATABASE_NAME};" | run_mysql || die "Failed to create database"

	info "Creating user and granting privileges..."
	echo "CREATE USER IF NOT EXISTS '${DATABASE_NAME}'@'localhost' IDENTIFIED BY '${DATABASE_NAME}';" | run_mysql || warn "User may already exist"
	echo "GRANT ALL PRIVILEGES ON ${DATABASE_NAME}.* TO '${DATABASE_NAME}'@'localhost';" | run_mysql || die "Failed to grant privileges"

	success "Database '${DATABASE_NAME}' created successfully!"
}

cmd_db_drop() {
	ensure_database_config

	if ! confirm "Are you sure you want to drop database '${DATABASE_NAME}'?"; then
		echo "Aborted."
		exit 0
	fi

	get_mysql_auth

	info "Dropping database '${DATABASE_NAME}'..."
	echo "DROP DATABASE IF EXISTS ${DATABASE_NAME};" | run_mysql || die "Failed to drop database"

	success "Database '${DATABASE_NAME}' dropped successfully!"
}

cmd_db_import() {
	local dump_file="${1:-}"
	local uncompress="${2:-0}"

	ensure_database_config

	if [[ -z "$dump_file" ]]; then
		# Download from S3
		require_cmd aws "Please install the AWS CLI: https://aws.amazon.com/cli/"
		ensure_aws_config

		mkdir -p "${WORK_DIR}/.databases"

		# Get yesterday's date (cross-platform)
		local yesterday
		if [[ "$OSTYPE" == "darwin"* ]]; then
			yesterday=$(date -v-1d +%Y-%m-%d)
		else
			yesterday=$(date -d "-1 day" +%Y-%m-%d)
		fi

		local backup_name="${yesterday}-${DATABASE_NAME}.sql.gz"
		local local_path="${WORK_DIR}/.databases/${backup_name}"

		info "Downloading backup '${backup_name}' from S3..."
		aws s3 cp "s3://vc-aws-infrastructure/rds-backups/${backup_name}" "$local_path" \
			--region "${AWS_REGION}" --profile "${AWS_PROFILE}" \
			|| die "Failed to download backup"

		if [[ "$uncompress" == "1" ]]; then
			info "Uncompressing..."
			gunzip -f "$local_path" || die "Failed to uncompress backup"
			dump_file="${local_path%.gz}"
		else
			dump_file="$local_path"
		fi
	fi

	if [[ ! -f "$dump_file" ]]; then
		die "Dump file not found: ${dump_file}"
	fi

	get_mysql_auth

	info "Importing '${dump_file}' into '${DATABASE_NAME}'..."
	if [[ "$dump_file" == *.gz ]]; then
		zcat "$dump_file" | run_mysql "$DATABASE_NAME" || die "Failed to import dump"
	else
		run_mysql "$DATABASE_NAME" < "$dump_file" || die "Failed to import dump"
	fi

	success "Database imported successfully!"
}

cmd_db_clean() {
	local db_dir="${WORK_DIR}/.databases"

	if [[ ! -d "$db_dir" ]]; then
		info "No .databases directory found."
		exit 0
	fi

	local count
	count=$(find "$db_dir" -type f | wc -l | tr -d ' ')

	if [[ "$count" == "0" ]]; then
		info "No files in .databases directory."
		exit 0
	fi

	echo "Found ${count} file(s) in ${db_dir}:"
	ls -lh "$db_dir"
	echo

	if confirm "Delete all files in .databases?"; then
		rm -rf "${db_dir:?}"/*
		success "Cleaned .databases directory."
	else
		echo "Aborted."
	fi
}

# ─────────────────────────────────────────────────────────────────────────────
# AWS Commands
# ─────────────────────────────────────────────────────────────────────────────

cmd_logs() {
	local stage="${1:-app}"

	require_cmd aws "Please install the AWS CLI: https://aws.amazon.com/cli/"
	ensure_logs_config "$stage"

	local log_group_var="CLOUD_WATCH_LOG_GROUP_${stage}"
	local log_group="${!log_group_var}"

	info "Tailing logs from '${log_group}' (profile=${AWS_PROFILE}, region=${AWS_REGION})..."
	aws logs tail --follow --region "${AWS_REGION}" --profile "${AWS_PROFILE}" "$log_group"
}

# ─────────────────────────────────────────────────────────────────────────────
# Help
# ─────────────────────────────────────────────────────────────────────────────

show_help() {
	cat <<-EOF
	${BOLD}Variocube Developer Tools v${VERSION}${RESET}

	${BOLD}USAGE${RESET}
	    ./devtools.sh [command] [options]

	${BOLD}COMMANDS${RESET}
	    ${BOLD}(no args)${RESET}      Install or update devtools
	    ${BOLD}update${RESET}         Alias for install/update
	                   --commit    Commit changes after install
	                   --push      Commit and push changes after install

	    ${BOLD}db:create${RESET}      Create local MySQL database
	    ${BOLD}db:drop${RESET}        Drop local MySQL database
	    ${BOLD}db:import${RESET}      Import database dump
	                   -d <file>   Use specific dump file (skip S3 download)
	                   -u          Uncompress downloaded dump

	    ${BOLD}db:clean${RESET}       Delete all files in .databases directory

	    ${BOLD}logs${RESET}           Tail CloudWatch logs
	                   -s <stage>  Specify stage (default: app)

	    ${BOLD}help${RESET}           Show this help message

	${BOLD}CONFIGURATION${RESET}
	    Configuration is stored in .vc and created interactively when needed.
	    Available settings:
	        DATABASE_NAME              Database name for local MySQL
	        VC_AWS_PROFILE             AWS CLI profile name
	        VC_AWS_REGION              AWS region
	        CLOUD_WATCH_LOG_GROUP_*    CloudWatch log group per stage

	${BOLD}EXAMPLES${RESET}
	    # Fresh install
	    wget -qO- ${GITHUB_RAW_URL} | bash

	    # Update existing installation
	    ./devtools.sh update

	    # Create database (prompts for name if not configured)
	    ./devtools.sh db:create

	    # Import latest backup from S3
	    ./devtools.sh db:import

	    # Import specific dump file
	    ./devtools.sh db:import -d backup.sql.gz

	    # Tail production logs
	    ./devtools.sh logs -s prod

	${BOLD}NOTES${RESET}
	    For Linux users: Leave the MySQL password empty to use sudo for root access.

	EOF
}

# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

main() {
	# Handle piped execution (wget ... | bash)
	if [[ ! -f "${BASH_SOURCE[0]:-}" ]] || [[ "${BASH_SOURCE[0]}" == "/dev/stdin" ]]; then
		# Running from pipe - bootstrap by downloading script
		local temp_script
		temp_script=$(mktemp)
		trap 'rm -f "$temp_script"' EXIT

		require_cmd wget
		wget -q "$GITHUB_RAW_URL" -O "$temp_script" || die "Failed to download devtools.sh"
		chmod +x "$temp_script"
		exec bash "$temp_script" "$@"
	fi

	# Determine script location for update command
	local script_dir
	script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

	# Parse long options first
	local do_commit=0
	local do_push=0
	local args=()

	for arg in "$@"; do
		case "$arg" in
			--commit) do_commit=1 ;;
			--push) do_commit=1; do_push=1 ;;
			*) args+=("$arg") ;;
		esac
	done
	set -- "${args[@]}"

	# Parse command
	local command="${1:-}"
	shift || true

	# Parse options
	local dump_file=""
	local uncompress=0
	local stage="app"

	while getopts "hd:us:" opt 2>/dev/null || true; do
		case "${opt:-}" in
			h) show_help; exit 0 ;;
			d) dump_file="$OPTARG" ;;
			u) uncompress=1 ;;
			s) stage="$OPTARG" ;;
			\?) break ;;
			*) break ;;
		esac
	done

	case "$command" in
		""|update)
			# Install or update devtools
			if [[ -d "$DEVTOOLS_DIR" ]]; then
				info "Updating devtools in ${script_dir}..."
			else
				info "Installing devtools in ${WORK_DIR}..."
			fi
			install_devtools "$WORK_DIR" "$do_commit" "$do_push"
			;;
		db:create)
			cmd_db_create
			;;
		db:drop)
			cmd_db_drop
			;;
		db:import)
			cmd_db_import "$dump_file" "$uncompress"
			;;
		db:clean)
			cmd_db_clean
			;;
		logs)
			cmd_logs "$stage"
			;;
		help|--help|-h)
			show_help
			;;
		*)
			echo "Unknown command: $command"
			echo
			show_help
			exit 1
			;;
	esac
}

main "$@"
