# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a bare-bones database implementation (RookieDB) for Berkeley CS186 Spring 2026. It supports executing simple transactions in series and will be extended throughout the semester with:
- B+ tree indices (Project 2)
- Efficient join algorithms and query optimization (Project 3)
- Multigranularity locking for concurrent transactions (Project 4)
- Database recovery using ARIES (Project 5)

## Build and Development Commands

### Maven Commands
```bash
# Compile the project
mvn compile

# Run all tests
mvn test

# Run tests for a specific project
mvn test -Dproj=2  # For Project 2

# Run tests with specific profile (public tests)
mvn test -Ppublic

# Run tests with system tests
mvn test -Psystem

# Run tests with hidden tests
mvn test -Phidden

# Run uncategorized tests
mvn test -Puncategorized

# Run all tests (no group filtering)
mvn test -Pall
```

### Parser Generation
```bash
# Generate parser from JJTree grammar
./parsergen.sh
```

### Running the CLI
```bash
# Run the command line interface
# (This requires the database to be built first)
java -cp target/classes edu.berkeley.cs186.database.cli.CommandLineInterface
```

## Project Structure

### Core Components

- **Database.java** - Main entry point, coordinates all components
- **Transaction.java** - Public interface for database operations
- **TransactionContext.java** - Internal interface for transaction state

### Key Directories

- **cli/parser/** - Auto-generated SQL parser (from RookieParser.jjt)
- **cli/visitor/** - AST visitors for parsing and query construction
- **common/** - Utility classes and iterators (BacktrackingIterator, etc.)
- **databox/** - Type system implementation (DataBox, Type)
- **index/** - B+ tree index implementation (skeleton for Project 2)
- **io/** - Disk space management (DiskSpaceManager, page allocation)
- **memory/** - Buffer management (BufferManager, eviction policies)
- **query/** - Query execution operators (SequentialScan, Join, etc.)
- **table/** - Heap file storage, records, and tables
- **concurrency/** - Lock manager and transaction coordination (skeleton for Project 4)
- **recovery/** - ARIES recovery implementation (skeleton for Project 5)

### Test Structure

Tests are organized by project and category:
- `Proj[1-5]Tests` - Tests for specific projects
- `PublicTests` - Basic functionality tests
- `HiddenTests` - Advanced implementation tests
- `SystemTests` - Integration tests
- `StudentTests` - Custom student tests

To run tests for a specific assignment in IntelliJ:
1. Create a JUnit configuration with:
   - Test kind: Category
   - Category: `edu.berkeley.cs186.database.categories.Proj[1-5]Tests`
   - Search for tests: In whole project

## Development Notes

- Code is built and tested in a Docker container with Java 8
- Only specific files are editable per assignment - changes to other files are discarded during testing
- The database uses a custom SQL-like parser generated from JJTree grammar
- All database operations must be performed within transactions
- The buffer manager uses a write-back cache with configurable eviction policies (LRU or Clock)
- Tables are stored as heap files using PageDirectory for management

## Test Execution

When running tests, note the JVM memory limits set in the Maven configuration:
```xml
<argLine> -Xms32m -Xmx32m </argLine>
```
This limits test memory to 32MB, which affects how much data can be processed during tests.