# Contributing to Data Universe (Subnet 13)

Thank you for your interest in contributing to Bittensor Subnet 13: Data Universe! This document provides guidelines for contributing to this project.

## Table of Contents

- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Contributing Guidelines](#contributing-guidelines)
- [Pull Request Process](#pull-request-process)
- [Code Standards](#code-standards)
- [Testing](#testing)
- [Data Compliance](#data-compliance)
- [Community](#community)

## Getting Started

Data Universe is a data-scraping and sentiment analysis subnet that collects and stores data from X (Twitter), Reddit, and YouTube. Before contributing, please:

1. Read the [README.md](README.md) to understand the project
2. Review the [Miner Policy](docs/miner_policy.md) for data compliance requirements
3. Check existing [issues](https://github.com/macrocosm-os/data-universe/issues) and [pull requests](https://github.com/macrocosm-os/data-universe/pulls)

## Development Setup

### Prerequisites

- Python 3.9+
- Git
- Access to appropriate API keys for data sources (if testing scrapers)

### Installation

1. **Fork the repository**: Click the "Fork" button at the top right of the GitHub page to create your own copy
2. Clone your fork:
   ```bash
   git clone https://github.com/your-username/data-universe.git
   cd data-universe
   ```

3. Install dependencies:
   ```bash
   pip install -e
   ```

4. Set up your development environment:
   ```bash
   # Follow the setup instructions in docs/miner.md or docs/validator.md
   ```

## Contributing Guidelines

### Types of Contributions

We welcome the following types of contributions:

- **Bug fixes**: Report and fix bugs
- **Feature enhancements**: Improve existing features
- **New data sources**: Add support for additional scraping sources
- **Documentation**: Improve or add documentation
- **Performance improvements**: Optimize existing code
- **Testing**: Add or improve test coverage

### Before You Start

1. **Check existing work**: Search issues and PRs to avoid duplicating effort
2. **Create an issue**: For significant changes, create an issue first to discuss the approach
3. **Follow data compliance**: Ensure your contributions comply with our [Miner Data Compliance Policy](README.md#must-read-miner-data-compliance-policy)

## Pull Request Process

### Target Branch

**All pull requests should target the `dev` branch, not `main`.**

The `dev` branch is our integration branch where new features and fixes are tested before being merged to `main`. This helps maintain stability in the production branch.

### 1. Prepare Your Changes

- Create a feature branch from `dev`: `git checkout dev && git pull && git checkout -b feature/your-feature-name`
- Make your changes following our [code standards](#code-standards)
- Add tests for new functionality
- Update documentation as needed

### 2. Test Your Changes

- Run existing tests to ensure nothing breaks
- Add new tests for your changes
- Verify compliance with data protection requirements

### 3. Submit Your Pull Request

- Push your branch to your fork
- Create a pull request with:
  - Clear title and description
  - Reference to related issues
  - Description of changes made
  - Testing instructions
  - Screenshots (if applicable)

### 4. Review Process

- All PRs require review from maintainers
- Address any feedback promptly
- Ensure CI checks pass
- Maintain a clean commit history

## Code Standards

### Python Code Style

- Follow PEP 8 guidelines
- Use meaningful variable and function names
- Add docstrings for classes and functions
- Keep functions focused and modular

### Commit Messages

Use clear, descriptive commit messages:
```
feat: add YouTube multilanguage support
fix: resolve X API rate limiting issue
docs: update miner setup instructions
test: add unit tests for Reddit scraper
```

### File Organization

- Place new scrapers in appropriate subdirectories under `scraping/`
- Add configuration files to `scraping/config/`
- Update relevant documentation in `docs/`
- Add tests in `tests/` directory

## Testing


### Writing Tests

- Write unit tests for new functions
- Include integration tests for new data sources
- Mock external API calls
- Test edge cases and error handling

## Data Compliance

All contributions must comply with our data compliance policy:

### Prohibited Content

Never collect, process, or transmit:
- Child abuse material or exploitation
- Hate speech or extremist content
- Copyrighted material without permission
- Explicit or harmful content
- Personal data without proper legal basis

### GDPR Compliance

- Implement data minimization principles
- Ensure proper data security measures
- Include appropriate privacy protections
- Document any personal data handling

### Best Practices

- Anonymize data where possible
- Implement proper error handling
- Use secure storage practices
- Follow platform-specific terms of service

## Community

### Communication Channels

- **GitHub Issues**: Bug reports and feature requests
- **Discord**: Join the [Bittensor Discord](https://discord.gg/bittensor) for discussions
- **Team Contact**: Reach out to @arrmlet, @ewekazoo, or the Macrocosmos team

### Code of Conduct

- Be respectful and inclusive
- Focus on constructive feedback
- Help others learn and contribute
- Follow ethical data handling practices

### Getting Help

- Check existing documentation first
- Search closed issues for solutions
- Ask questions in GitHub issues or Discord
- Contact team members for guidance

## Recognition

Contributors will be recognized in:
- Release notes for significant contributions
- Project documentation
- Community acknowledgments

## License

By contributing to this project, you agree that your contributions will be licensed under the same license as the project (MIT License).

---

For questions about contributing, please reach out to the team on Discord or create an issue. We appreciate your interest in improving Data Universe!