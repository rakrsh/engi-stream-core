# GitHub Copilot Instructions for Engi-Stream Core

## Project Overview
Engi-Stream Core is a high-performance data ingestion and orchestration system designed for agentic workflows. It leverages Python, Docker, and Kubernetes for scalable data processing.

## General Principles
- **Clarity over Cleverness**: Write code that is easy to read and maintain.
- **Type Safety**: Use Python type hints consistently.
- **Test-Driven**: Prioritize writing unit and integration tests.
- **Documentation**: Maintain up-to-date docstrings and markdown documentation.

## Coding Standards
- **Python**: Follow PEP 8. Use `black` for formatting and `isort` for imports.
- **Documentation**: Use Google-style docstrings.
- **Error Handling**: Use custom exceptions and robust error logging.

## Agentic Workflows
- When working on agents, ensure they are modular and follow the "Observe-Orient-Decide-Act" (OODA) loop where applicable.
- Agents should communicate via well-defined interfaces.

## CI/CD and Quality
- All changes must pass pre-commit hooks.
- Security and license checks are mandatory in the CI pipeline.
