# Remove Duplicates & Cleanup System

A comprehensive Python system for organizing, removing duplicates, and cleaning up files and data with advanced strategies and monitoring capabilities.

## Overview

This system provides three main components:

1. **Remove Duplicates** (`remove_duplicates.py`) - Advanced duplicate removal with multiple strategies
2. **Cleanup System** (`cleanup_system.py`) - Comprehensive cleanup and maintenance utilities  
3. **Organize System** (`organize_system.py`) - Advanced file and data organization

## Features

### Remove Duplicates
- **Multiple Strategies**: Exact, fuzzy, semantic, and hash-based duplicate removal
- **File System Support**: Scan and remove duplicate files by content hash
- **List Processing**: Remove duplicates from lists with complex items
- **Nested Structure Support**: Handle nested lists and dictionaries
- **Performance Optimized**: Efficient algorithms with progress tracking

### Cleanup System
- **Process Management**: Advanced process cleanup with multiple strategies (graceful, force, timeout)
- **Cache Management**: Intelligent cache cleanup with TTL and LRU eviction
- **File System Cleanup**: Temporary file cleanup, log file rotation, empty directory removal
- **Integrated Monitoring**: Background monitoring with automatic cleanup
- **Comprehensive History**: Detailed cleanup operation tracking

### Organize System
- **File Organization**: Organize files by type, date, size, or custom rules
- **Data Organization**: Group, sort, filter, and transform data structures
- **Deduplication Integration**: Combined duplicate removal and organization
- **Flexible Rules**: Custom organization rules with conditions
- **Schema Normalization**: Data structure normalization and validation

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd remove-duplicates-cleanup-system

# Install dependencies
pip install -r requirements.txt

# Run tests
python -m pytest tests/
```

## Requirements

- Python 3.7+
- psutil (for process management)
- Optional: Additional dependencies for specific features

## Quick Start

### Basic Duplicate Removal

```python
from remove_duplicates import DuplicateRemover

remover = DuplicateRemover()

# Remove exact duplicates
items = ["apple", "banana", "apple", "orange"]
cleaned = remover.remove_duplicates(items)
print(f"Removed {remover.removed_count} duplicates")

# Remove fuzzy duplicates (for strings)
fuzzy_items = ["Apple Inc.", "Apple Inc", "APPLE INC"]
cleaned = remover.remove_duplicates(fuzzy_items, strategy='fuzzy', threshold=0.8)
```

### File Duplicate Removal

```python
from remove_duplicates import FileDuplicateRemover

# Scan and remove duplicate files
file_remover = FileDuplicateRemover("/path/to/directory")
duplicates = file_remover.scan_directory()
results = file_remover.remove_file_duplicates(keep_first=True)
```

### Process Cleanup

```python
from cleanup_system import ProcessCleanupManager

process_manager = ProcessCleanupManager()

# Register a process
process_manager.register_process(pid=1234, command="my_process")

# Cleanup with different strategies
process_manager.cleanup_process(1234, strategy='graceful')
process_manager.cleanup_process(1234, strategy='force')
```

### File Organization

```python
from organize_system import FileOrganizer

organizer = FileOrganizer("/path/to/directory")

# Add custom organization rule
organizer.add_organization_rule({
    "name": "organize_images",
    "pattern": "*.jpg",
    "destination": "images/{year}/{month}",
    "condition": {"size_min": 1024}
})

# Organize directory
results = organizer.organize_directory(dry_run=True)
```

### Integrated System

```python
from organize_system import IntegratedOrganizationSystem

# Create integrated system
system = IntegratedOrganizationSystem("/base/directory")

# Comprehensive organization
config = {
    "files": {
        "organize_by_type": True,
        "organize_by_date": True,
        "deduplicate": True
    },
    "data": {
        "sample_data": [
            {"type": "group_by", "params": {"key": "category"}}
        ]
    }
}

results = system.organize_system(config)
```

## Advanced Usage

### Custom Cleanup Strategies

```python
from cleanup_system import IntegratedCleanupSystem

cleanup_system = IntegratedCleanupSystem()

# Start monitoring
cleanup_system.start_monitoring(interval=300)

# Add cleanup rule
cleanup_system.fs_manager.add_cleanup_rule({
    "name": "temp_cleanup",
    "pattern": "*.tmp",
    "action": "delete",
    "condition": {"max_age": 3600}
})
```

### Data Organization

```python
from organize_system import DataOrganizer

data_organizer = DataOrganizer()

# Group data by category
grouped = data_organizer.organize_data(
    data, 
    "group_by", 
    key="category", 
    sort_groups=True
)

# Normalize data structure
schema = {
    "name": {"type": "string", "required": True},
    "age": {"type": "int", "default": 0},
    "active": {"type": "bool", "default": True}
}

normalized = data_organizer.normalize_data_structure(data, schema)
```

## Configuration

### Environment Variables

```bash
# Logging level
export LOG_LEVEL=INFO

# Default cleanup intervals
export CLEANUP_INTERVAL=300
export CACHE_TTL=3600
```

### Configuration Files

Create `config.json` for default settings:

```json
{
    "cleanup": {
        "interval": 300,
        "auto_start": true,
        "strategies": ["graceful", "force"]
    },
    "organization": {
        "default_rules": true,
        "preserve_structure": false
    },
    "duplicates": {
        "default_strategy": "exact",
        "fuzzy_threshold": 0.8
    }
}
```

## Monitoring and Logging

The system provides comprehensive logging and monitoring:

```python
import logging

# Enable debug logging
logging.basicConfig(level=logging.DEBUG)

# Get system status
status = cleanup_system.get_system_status()
print(json.dumps(status, indent=2))
```

## Performance Considerations

- **Large Directories**: Use chunked processing for directories with many files
- **Memory Usage**: Monitor cache sizes and adjust limits as needed
- **Concurrent Operations**: Use threading for parallel processing when possible
- **I/O Optimization**: Batch file operations to reduce disk access

## Troubleshooting

### Common Issues

1. **Permission Errors**: Ensure proper file system permissions
2. **Memory Issues**: Adjust cache sizes and batch processing limits
3. **Process Cleanup**: Some processes may require elevated privileges
4. **File Locks**: Close file handles before attempting operations

### Debug Mode

Enable debug mode for detailed logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## API Reference

### DuplicateRemover

```python
class DuplicateRemover:
    def remove_duplicates(self, items, strategy='exact', **kwargs)
    def _remove_exact_duplicates(self, items, **kwargs)
    def _remove_fuzzy_duplicates(self, items, threshold=0.8, **kwargs)
    def _remove_semantic_duplicates(self, items, **kwargs)
    def _remove_hash_based_duplicates(self, items, **kwargs)
```

### ProcessCleanupManager

```python
class ProcessCleanupManager:
    def register_process(self, pid, command, process_obj=None, metadata=None)
    def cleanup_process(self, pid, strategy='graceful', timeout=30)
    def cleanup_all_processes(self, strategy='graceful')
    def get_cleanup_stats(self)
```

### FileOrganizer

```python
class FileOrganizer:
    def add_organization_rule(self, rule)
    def organize_directory(self, source_dir=None, dry_run=False)
    def organize_by_type(self, source_dir=None, dry_run=False)
    def organize_by_date(self, source_dir=None, date_format="%Y/%m", dry_run=False)
    def deduplicate_and_organize(self, source_dir=None, strategy="move")
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Changelog

### v1.0.0
- Initial release
- Core duplicate removal functionality
- Process cleanup system
- File organization capabilities
- Integrated monitoring system

## Support

For issues and questions:
- Create an issue on GitHub
- Check the troubleshooting section
- Review the API documentation

---

**Note**: This system is designed for production use with comprehensive error handling and monitoring. Always test with dry-run modes before performing destructive operations.
