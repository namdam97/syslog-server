# AI DevKit Custom Commands

## /new-requirement
**Purpose**: Generate a new requirement document.
**Prompt**: Analyze the requirements for [feature] and create a detailed docs/ai/requirements.md file following the template.

## /execute-plan
**Purpose**: Implement tasks from docs/ai/plan.md one by one.
**Prompt**: Look at docs/ai/plan.md and implement the next unchecked task.

## /check-implementation
**Purpose**: Verify if the implementation matches the requirements and design.
**Prompt**: Compare the current code with docs/ai/requirements.md and docs/ai/design.md.

## /code-review
**Purpose**: Perform a deep code review.
**Prompt**: Review the selected code for security, performance, and best practices.

## /writing-test
**Purpose**: Generate unit and integration tests.
**Prompt**: Write comprehensive tests for the current file to achieve 80%+ coverage.
