# File Organizer

## Short Description
🧹 Advanced Python system for removing duplicates, cleaning up files, and organizing data with intelligent strategies and real-time monitoring - now unified in a single powerful tool.

## Extended Description

A comprehensive Python toolkit designed to tackle file and data duplication, system cleanup, and organization challenges. This system provides a unified solution that combines three powerful modules working together to maintain clean, organized, and efficient digital environments.

### 🎯 Key Features

**🔄 Ultimate File Organization**
- **Smart File Categorization**: Automatically organizes files by type (images, documents, videos, audio, code, archives)
- **Date-Based Organization**: Optional organization by year/month folder structure
- **Custom Rules**: Support for custom organization patterns and conditions
- **MIME Type Detection**: Intelligent file type recognition beyond extensions
- **Conflict Resolution**: Handles filename conflicts with automatic numbering

**🗑️ Advanced Duplicate Removal**
- **Multiple Deduplication Strategies**: SHA256 hash-based exact matching
- **File System Scanning**: Comprehensive directory traversal with content comparison
- **Space Optimization**: Calculate and report space savings from duplicate removal
- **Safe Handling**: Move duplicates to separate folder or delete permanently
- **Progress Tracking**: Real-time feedback on duplicate detection progress

**🧹 Intelligent System Cleanup**
- **Automated Cleanup Rules**: Built-in rules for temp files (*.tmp), cache (*.cache), and logs (*.log)
- **Age-Based Filtering**: Clean files older than specified thresholds (1 day, 7 days, etc.)
- **Size-Based Filtering**: Target files within specific size ranges
- **Custom Rule Engine**: Add custom cleanup patterns and conditions
- **Space Recovery**: Track and report freed disk space

**⚙️ Unified Management**
- **Single Command Execution**: Run all operations with one command
- **Dry Run Mode**: Preview changes before applying them
- **Selective Operations**: Choose which operations to perform
- **Detailed Reporting**: Comprehensive statistics and operation history
- **Export Capabilities**: Save results to JSON for analysis

## 📁 Project Structure

```
├── ultimate_file_organizer.py    # Main unified tool (NEW)
├── organize_system.py           # Original file organization module
├── remove_duplicates.py         # Original duplicate removal module
├── cleanup_system.py            # Original cleanup module
├── system_wide_organization.py  # System-wide organization script
└── README_NEW.md               # This documentation
```

## 🚀 Quick Start

### Installation
```bash
# Clone the repository
git clone https://github.com/iblu23/system_cleanup/
cd hexstrike-ai

# Install dependencies (if needed)
pip install psutil
```

### Basic Usage

**Run Complete Organization:**
```bash
# Organize current directory
python3 ultimate_file_organizer.py

# Organize specific directory
python3 ultimate_file_organizer.py /path/to/directory
```

**Advanced Options:**
```bash
# Preview changes without executing
python3 ultimate_file_organizer.py --dry-run

# Skip specific operations
python3 ultimate_file_organizer.py --no-deduplicate --no-cleanup

# Also organize by date
python3 ultimate_file_organizer.py --organize-by-date

# Export results to file
python3 ultimate_file_organizer.py --export results.json
```

## 📋 Command Line Options

| Option | Description |
|--------|-------------|
| `directory` | Directory to organize (default: current) |
| `--dry-run` | Show what would be done without making changes |
| `--no-organize` | Skip file organization |
| `--no-deduplicate` | Skip duplicate removal |
| `--no-cleanup` | Skip system cleanup |
| `--organize-by-date` | Also organize files by date |
| `--export` | Export results to JSON file |

## 🔧 Individual Module Usage

### File Organization Only
```python
from ultimate_file_organizer import FileOrganizer

organizer = FileOrganizer("/path/to/directory")
results = organizer.organize_by_type()
print(f"Organized {results['files_processed']} files")
```

### Duplicate Removal Only
```python
from ultimate_file_organizer import FileDuplicateRemover

remover = FileDuplicateRemover("/path/to/directory")
duplicates = remover.scan_directory()
if duplicates:
    results = remover.remove_duplicates()
    print(f"Removed {results['duplicates_removed']} duplicates")
```

### System Cleanup Only
```python
from ultimate_file_organizer import FileSystemCleanupManager

cleanup = FileSystemCleanupManager()
cleanup.add_cleanup_rule({
    'name': 'clean_temp_files',
    'pattern': '*.tmp',
    'action': 'delete',
    'condition': {'age_min': 86400}
})
results = cleanup.apply_cleanup_rules("/path/to/directory")
```

## 📊 Output Examples

### Organization Results
```
📁 Organizing files by type...
   ✅ 1,247 files processed
   📂 Categories: ['images', 'documents', 'code', 'videos']
   ⏱️  Time: 2.3s
```

### Duplicate Removal Results
```
🗑️  Removing duplicates...
   ✅ 23 duplicates removed
   💾 Space saved: 156.7 MB
   ⏱️  Time: 5.1s
```

### Cleanup Results
```
🧹  Running system cleanup...
   ✅ 47 files cleaned
   💾 Space saved: 23.4 MB
   ⏱️  Time: 0.8s
```

### Final Summary
```
📊 ULTIMATE ORGANIZATION SUMMARY
==================================================
📁 Files Organized: 1,247
📂 Categories Created: 4
🗑️  Duplicates Removed: 23
🧹 Files Cleaned: 47
💾 Total Space Saved: 180.1 MB
⚙️  Operations Completed: 3
```

## 🎯 Use Cases

### 📱 **iPhone Photo Management**
- Organize photos by type (JPG, HEIC, MOV)
- Remove duplicate photos and videos
- Clean up temporary files
- Example: `python3 ultimate_file_organizer.py ~/iPhone/DCIM/100APPLE`

### 💻 **Development Project Cleanup**
- Organize source files by language
- Remove duplicate dependencies
- Clean build artifacts and temp files
- Example: `python3 ultimate_file_organizer.py ~/projects/myapp`

### 🗂️ **Document Management**
- Organize documents by type (PDF, DOC, TXT)
- Remove duplicate files
- Clean old drafts and temp files
- Example: `python3 ultimate_file_organizer.py ~/Documents`

### 🖥️ **System Maintenance**
- Clean temporary directories (/tmp, /var/tmp)
- Organize application files in /opt
- Remove duplicate system files
- Example: `sudo python3 ultimate_file_organizer.py /tmp`

## ⚡ Performance Features

- **Optimized Scanning**: Efficient directory traversal with parallel processing
- **Memory Management**: Chunked file hashing for large files
- **Progress Tracking**: Real-time feedback on long operations
- **Error Handling**: Robust error recovery and reporting
- **Logging**: Detailed operation logging for debugging

## 🔒 Safety Features

- **Dry Run Mode**: Preview all changes before execution
- **Conflict Resolution**: Automatic handling of filename conflicts
- **Backup Strategy**: Move duplicates to separate folder instead of deletion
- **Permission Handling**: Graceful handling of permission issues
- **Rollback Support**: Operation history for potential rollback

## 🛠️ Customization

### Custom Organization Rules
```python
organizer.add_organization_rule({
    'name': 'organize_images_by_date',
    'pattern': '*.jpg',
    'destination': 'images/{year}/{month}',
    'condition': {'size_min': 1024}
})
```

### Custom Cleanup Rules
```python
cleanup.add_cleanup_rule({
    'name': 'clean_old_downloads',
    'pattern': '*.download',
    'action': 'delete',
    'condition': {'age_min': 604800}  # 7 days
})
```

## 📈 Monitoring and Reporting

- **Operation History**: Track all organization operations
- **Statistics**: Detailed file processing statistics
- **Space Analysis**: Before/after disk space comparison
- **Export Support**: Save results to JSON for further analysis
- **Progress Indicators**: Real-time progress for long operations

## 🤝 Integration

The Ultimate File Organizer can be easily integrated into:
- **CI/CD Pipelines**: Automated project cleanup
- **Backup Systems**: Pre-backup organization
- **File Management Tools**: Enhanced file operations
- **System Scripts**: Regular maintenance tasks
- **Applications**: Embedded file management

## 📝 Requirements

- Python 3.7+
- psutil (for system information)
- Standard library modules: os, shutil, json, hashlib, pathlib, datetime

## 🔄 Migration from Individual Scripts

If you were using the individual scripts before:

**Old way:**
```bash
python3 organize_system.py
python3 remove_duplicates.py  
python3 cleanup_system.py
```

**New way:**
```bash
python3 ultimate_file_organizer.py
```

The new unified tool provides all the same functionality with:
- Better integration between components
- Unified configuration
- Comprehensive reporting
- Simplified usage

## 🐛 Troubleshooting

### Common Issues

**Permission Denied:**
```bash
# Use sudo for system directories
sudo python3 ultimate_file_organizer.py /tmp
```

**Memory Usage with Large Directories:**
```bash
# Use dry-run first to estimate
python3 ultimate_file_organizer.py --dry-run /large/directory
```

**File Access Errors:**
- Check file permissions
- Ensure files aren't in use
- Use appropriate user privileges

## 📄 License

This project is open source and available under the MIT License.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit pull requests, report issues, or suggest features.

## 📞 Support

For support and questions:
- Check the troubleshooting section
- Review operation logs
- Use dry-run mode to test configurations
- Export results for analysis

---

**Ultimate File Organizer** - The complete solution for file organization, deduplication, and system cleanup. 🚀
