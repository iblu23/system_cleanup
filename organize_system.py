#!/usr/bin/env python3
"""
System Organization Manager - Advanced file and data organization utilities
"""

import os
import shutil
import json
import time
import hashlib
import logging
from typing import Dict, List, Any, Optional, Tuple, Callable
from pathlib import Path
from datetime import datetime
from collections import defaultdict, Counter
import mimetypes
import re

logger = logging.getLogger(__name__)

class FileOrganizer:
    """Advanced file organization and categorization system"""
    
    def __init__(self, base_directory: str = None):
        self.base_directory = Path(base_directory) if base_directory else Path.cwd()
        self.organization_rules = []
        self.file_categories = self._default_categories()
        self.organization_history = []
        self.stats = {
            "files_organized": 0,
            "directories_created": 0,
            "space_organized": 0,
            "operations_completed": 0
        }
    
    def add_organization_rule(self, rule: Dict):
        """Add a file organization rule"""
        required_fields = ["name", "pattern", "destination", "condition"]
        for field in required_fields:
            if field not in rule:
                raise ValueError(f"Missing required field: {field}")
        
        self.organization_rules.append(rule)
        logger.info(f"📋 Added organization rule: {rule['name']}")
    
    def organize_directory(self, source_dir: str = None, dry_run: bool = False) -> Dict[str, Any]:
        """Organize files in directory according to rules"""
        source = Path(source_dir) if source_dir else self.base_directory
        
        if not source.exists():
            raise ValueError(f"Source directory does not exist: {source}")
        
        logger.info(f"🔄 Organizing directory: {source}")
        
        results = {
            "files_processed": 0,
            "files_moved": 0,
            "directories_created": 0,
            "errors": [],
            "operations": []
        }
        
        # Process all files
        for file_path in source.rglob("*"):
            if file_path.is_file():
                try:
                    operation = self._organize_file(file_path, source, dry_run)
                    if operation:
                        results["operations"].append(operation)
                        results["files_processed"] += 1
                        
                        if operation["action"] == "moved":
                            results["files_moved"] += 1
                        elif operation["action"] == "directory_created":
                            results["directories_created"] += 1
                
                except Exception as e:
                    error_msg = f"Error processing {file_path}: {e}"
                    logger.error(error_msg)
                    results["errors"].append(error_msg)
        
        # Update stats
        self.stats["files_organized"] += results["files_processed"]
        self.stats["directories_created"] += results["directories_created"]
        self.stats["operations_completed"] += 1
        
        # Record operation
        self._record_organization("directory_organization", results)
        
        logger.info(f"✅ Organization complete: {results['files_processed']} files processed")
        return results
    
    def organize_by_type(self, source_dir: str = None, dry_run: bool = False) -> Dict[str, Any]:
        """Organize files by type into categorized directories"""
        source = Path(source_dir) if source_dir else self.base_directory
        
        results = {
            "files_processed": 0,
            "files_moved": 0,
            "categories_created": set(),
            "errors": []
        }
        
        for file_path in source.rglob("*"):
            if file_path.is_file():
                try:
                    category = self._categorize_file(file_path)
                    if category:
                        destination_dir = source / category
                        destination_file = destination_dir / file_path.name
                        
                        if not dry_run:
                            # Create category directory if needed
                            destination_dir.mkdir(exist_ok=True)
                            results["categories_created"].add(category)
                            
                            # Move file
                            shutil.move(str(file_path), str(destination_file))
                        
                        results["files_processed"] += 1
                        results["files_moved"] += 1
                        
                        logger.debug(f"📁 Organized {file_path.name} to {category}")
                
                except Exception as e:
                    error_msg = f"Error organizing {file_path}: {e}"
                    logger.error(error_msg)
                    results["errors"].append(error_msg)
        
        results["categories_created"] = list(results["categories_created"])
        self._record_organization("type_organization", results)
        
        return results
    
    def organize_by_date(self, source_dir: str = None, date_format: str = "%Y/%m", dry_run: bool = False) -> Dict[str, Any]:
        """Organize files by modification date"""
        source = Path(source_dir) if source_dir else self.base_directory
        
        results = {
            "files_processed": 0,
            "files_moved": 0,
            "date_folders_created": set(),
            "errors": []
        }
        
        for file_path in source.rglob("*"):
            if file_path.is_file():
                try:
                    mod_time = datetime.fromtimestamp(file_path.stat().st_mtime)
                    date_folder = mod_time.strftime(date_format)
                    
                    destination_dir = source / date_folder
                    destination_file = destination_dir / file_path.name
                    
                    if not dry_run:
                        destination_dir.mkdir(exist_ok=True)
                        results["date_folders_created"].add(date_folder)
                        
                        # Handle name conflicts
                        if destination_file.exists():
                            counter = 1
                            stem = destination_file.stem
                            suffix = destination_file.suffix
                            while destination_file.exists():
                                destination_file = destination_dir / f"{stem}_{counter}{suffix}"
                                counter += 1
                        
                        shutil.move(str(file_path), str(destination_file))
                    
                    results["files_processed"] += 1
                    results["files_moved"] += 1
                
                except Exception as e:
                    error_msg = f"Error organizing {file_path}: {e}"
                    logger.error(error_msg)
                    results["errors"].append(error_msg)
        
        results["date_folders_created"] = list(results["date_folders_created"])
        self._record_organization("date_organization", results)
        
        return results
    
    def organize_by_size(self, source_dir: str = None, size_categories: Dict = None, dry_run: bool = False) -> Dict[str, Any]:
        """Organize files by size categories"""
        source = Path(source_dir) if source_dir else self.base_directory
        
        if size_categories is None:
            size_categories = {
                "small": 0,      # bytes
                "medium": 1024 * 1024,  # 1MB
                "large": 100 * 1024 * 1024,  # 100MB
                "xlarge": float('inf')
            }
        
        results = {
            "files_processed": 0,
            "files_moved": 0,
            "size_categories_used": set(),
            "errors": []
        }
        
        for file_path in source.rglob("*"):
            if file_path.is_file():
                try:
                    file_size = file_path.stat().st_size
                    category = self._get_size_category(file_size, size_categories)
                    
                    destination_dir = source / category
                    destination_file = destination_dir / file_path.name
                    
                    if not dry_run:
                        destination_dir.mkdir(exist_ok=True)
                        results["size_categories_used"].add(category)
                        
                        shutil.move(str(file_path), str(destination_file))
                    
                    results["files_processed"] += 1
                    results["files_moved"] += 1
                
                except Exception as e:
                    error_msg = f"Error organizing {file_path}: {e}"
                    logger.error(error_msg)
                    results["errors"].append(error_msg)
        
        results["size_categories_used"] = list(results["size_categories_used"])
        self._record_organization("size_organization", results)
        
        return results
    
    def deduplicate_and_organize(self, source_dir: str = None, strategy: str = "move") -> Dict[str, Any]:
        """Find duplicates and organize them"""
        from remove_duplicates import FileDuplicateRemover
        
        source = Path(source_dir) if source_dir else self.base_directory
        
        # Find duplicates
        duplicate_remover = FileDuplicateRemover(str(source))
        duplicates = duplicate_remover.scan_directory()
        
        results = {
            "duplicate_sets_found": len(duplicates),
            "duplicates_processed": 0,
            "space_saved": 0,
            "errors": []
        }
        
        # Create duplicates directory
        duplicates_dir = source / "_duplicates"
        if not duplicates_dir.exists():
            duplicates_dir.mkdir(exist_ok=True)
            results["directories_created"] = 1
        
        for hash_val, file_paths in duplicates.items():
            try:
                # Keep first file, move others to duplicates folder
                files_to_move = file_paths[1:]
                
                for file_path in files_to_move:
                    source_file = Path(file_path)
                    dest_file = duplicates_dir / source_file.name
                    
                    # Handle name conflicts
                    counter = 1
                    while dest_file.exists():
                        stem = dest_file.stem
                        suffix = dest_file.suffix
                        dest_file = duplicates_dir / f"{stem}_{counter}{suffix}"
                        counter += 1
                    
                    if strategy == "move":
                        shutil.move(str(source_file), str(dest_file))
                    elif strategy == "copy":
                        shutil.copy2(str(source_file), str(dest_file))
                    elif strategy == "delete":
                        source_file.unlink()
                    
                    results["duplicates_processed"] += 1
                    results["space_saved"] += source_file.stat().st_size
            
            except Exception as e:
                error_msg = f"Error processing duplicate set {hash_val}: {e}"
                logger.error(error_msg)
                results["errors"].append(error_msg)
        
        self._record_organization("deduplication", results)
        return results
    
    def _organize_file(self, file_path: Path, base_dir: Path, dry_run: bool) -> Optional[Dict]:
        """Organize a single file according to rules"""
        for rule in self.organization_rules:
            if self._matches_rule(file_path, rule):
                destination = self._get_destination(file_path, rule, base_dir)
                
                if destination:
                    if dry_run:
                        return {
                            "file": str(file_path),
                            "action": "would_move",
                            "destination": str(destination),
                            "rule": rule["name"]
                        }
                    else:
                        # Create destination directory if needed
                        destination.parent.mkdir(parents=True, exist_ok=True)
                        
                        # Handle name conflicts
                        if destination.exists():
                            counter = 1
                            stem = destination.stem
                            suffix = destination.suffix
                            while destination.exists():
                                destination = destination.parent / f"{stem}_{counter}{suffix}"
                                counter += 1
                        
                        # Move file
                        shutil.move(str(file_path), str(destination))
                        
                        return {
                            "file": str(file_path),
                            "action": "moved",
                            "destination": str(destination),
                            "rule": rule["name"]
                        }
        
        return None
    
    def _matches_rule(self, file_path: Path, rule: Dict) -> bool:
        """Check if file matches organization rule"""
        pattern = rule["pattern"]
        condition = rule.get("condition", {})
        
        # Check pattern match
        if not file_path.match(pattern):
            return False
        
        # Check conditions
        if "size_min" in condition and file_path.stat().st_size < condition["size_min"]:
            return False
        
        if "size_max" in condition and file_path.stat().st_size > condition["size_max"]:
            return False
        
        if "age_min" in condition:
            file_age = time.time() - file_path.stat().st_mtime
            if file_age < condition["age_min"]:
                return False
        
        if "age_max" in condition:
            file_age = time.time() - file_path.stat().st_mtime
            if file_age > condition["age_max"]:
                return False
        
        return True
    
    def _get_destination(self, file_path: Path, rule: Dict, base_dir: Path) -> Optional[Path]:
        """Get destination path for file according to rule"""
        destination_template = rule["destination"]
        
        # Replace placeholders
        destination = destination_template.replace("{name}", file_path.stem)
        destination = destination.replace("{ext}", file_path.suffix[1:] if file_path.suffix else "")
        destination = destination.replace("{parent}", file_path.parent.name)
        
        # Add date placeholders
        mod_time = datetime.fromtimestamp(file_path.stat().st_mtime)
        destination = destination.replace("{year}", str(mod_time.year))
        destination = destination.replace("{month}", f"{mod_time.month:02d}")
        destination = destination.replace("{day}", f"{mod_time.day:02d}")
        
        # Make relative to base directory
        if not os.path.isabs(destination):
            destination = base_dir / destination
        
        return destination / file_path.name
    
    def _categorize_file(self, file_path: Path) -> Optional[str]:
        """Categorize file by type"""
        # Check extension first
        ext = file_path.suffix.lower()
        
        for category, extensions in self.file_categories.items():
            if ext in extensions:
                return category
        
        # Use MIME type as fallback
        mime_type, _ = mimetypes.guess_type(str(file_path))
        if mime_type:
            if mime_type.startswith('image/'):
                return 'images'
            elif mime_type.startswith('video/'):
                return 'videos'
            elif mime_type.startswith('audio/'):
                return 'audio'
            elif mime_type.startswith('text/'):
                return 'documents'
            elif mime_type.startswith('application/'):
                if 'pdf' in mime_type:
                    return 'documents'
                elif 'zip' in mime_type or 'rar' in mime_type:
                    return 'archives'
                else:
                    return 'applications'
        
        return 'others'
    
    def _get_size_category(self, size: int, categories: Dict) -> str:
        """Get size category for file"""
        for category, min_size in sorted(categories.items(), key=lambda x: x[1], reverse=True):
            if size >= min_size:
                return category
        return "small"
    
    def _default_categories(self) -> Dict[str, List[str]]:
        """Default file categories"""
        return {
            'images': ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.svg', '.webp'],
            'documents': ['.pdf', '.doc', '.docx', '.txt', '.rtf', '.odt', '.xls', '.xlsx', '.ppt', '.pptx'],
            'videos': ['.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.webm'],
            'audio': ['.mp3', '.wav', '.flac', '.aac', '.ogg', '.wma'],
            'archives': ['.zip', '.rar', '.7z', '.tar', '.gz', '.bz2'],
            'code': ['.py', '.js', '.html', '.css', '.java', '.cpp', '.c', '.php', '.rb', '.go'],
            'applications': ['.exe', '.msi', '.dmg', '.app', '.deb', '.rpm'],
            'others': []
        }
    
    def _record_organization(self, operation: str, results: Dict):
        """Record organization operation in history"""
        self.organization_history.append({
            "operation": operation,
            "results": results,
            "timestamp": time.time()
        })
        
        # Keep only last 1000 records
        if len(self.organization_history) > 1000:
            self.organization_history = self.organization_history[-1000:]
    
    def get_organization_stats(self) -> Dict[str, Any]:
        """Get organization statistics"""
        return dict(self.stats)
    
    def get_organization_history(self, limit: int = 100) -> List[Dict]:
        """Get organization history"""
        return self.organization_history[-limit:]

class DataOrganizer:
    """Advanced data organization and structuring utilities"""
    
    def __init__(self):
        self.organization_strategies = {
            'group_by': self._group_by_strategy,
            'sort_by': self._sort_by_strategy,
            'filter_by': self._filter_by_strategy,
            'transform': self._transform_strategy
        }
    
    def organize_data(self, data: List[Dict], strategy: str, **kwargs) -> Dict[str, Any]:
        """Organize data using specified strategy"""
        if strategy not in self.organization_strategies:
            raise ValueError(f"Unknown strategy: {strategy}")
        
        return self.organization_strategies[strategy](data, **kwargs)
    
    def _group_by_strategy(self, data: List[Dict], key: str, sort_groups: bool = False) -> Dict[str, List[Dict]]:
        """Group data by specified key"""
        groups = defaultdict(list)
        
        for item in data:
            if key in item:
                groups[str(item[key])].append(item)
        
        result = dict(groups)
        
        if sort_groups:
            for group_key in result:
                result[group_key].sort(key=lambda x: x.get(key, ''))
        
        return result
    
    def _sort_by_strategy(self, data: List[Dict], key: str, reverse: bool = False) -> List[Dict]:
        """Sort data by specified key"""
        return sorted(data, key=lambda x: x.get(key, ''), reverse=reverse)
    
    def _filter_by_strategy(self, data: List[Dict], condition: Callable[[Dict], bool]) -> List[Dict]:
        """Filter data by condition"""
        return [item for item in data if condition(item)]
    
    def _transform_strategy(self, data: List[Dict], transformations: Dict[str, Callable]) -> List[Dict]:
        """Transform data items"""
        result = []
        
        for item in data:
            new_item = dict(item)
            
            for field, transform_func in transformations.items():
                if field in new_item:
                    try:
                        new_item[field] = transform_func(new_item[field])
                    except Exception as e:
                        logger.warning(f"⚠️  Transformation failed for {field}: {e}")
            
            result.append(new_item)
        
        return result
    
    def normalize_data_structure(self, data: List[Dict], schema: Dict) -> List[Dict]:
        """Normalize data structure according to schema"""
        normalized = []
        
        for item in data:
            normalized_item = {}
            
            for field, field_config in schema.items():
                field_type = field_config.get('type', 'string')
                default_value = field_config.get('default')
                required = field_config.get('required', False)
                
                if field in item:
                    value = item[field]
                    
                    # Type conversion
                    if field_type == 'int':
                        try:
                            value = int(value)
                        except:
                            value = default_value or 0
                    elif field_type == 'float':
                        try:
                            value = float(value)
                        except:
                            value = default_value or 0.0
                    elif field_type == 'bool':
                        value = bool(value)
                    elif field_type == 'list':
                        if not isinstance(value, list):
                            value = [value]
                    
                    normalized_item[field] = value
                
                elif required:
                    if default_value is not None:
                        normalized_item[field] = default_value
                    else:
                        raise ValueError(f"Required field {field} missing and no default provided")
            
            normalized.append(normalized_item)
        
        return normalized
    
    def deduplicate_data(self, data: List[Dict], key: str = None, fuzzy: bool = False) -> List[Dict]:
        """Remove duplicate data entries"""
        if key:
            # Remove duplicates by key
            seen_values = set()
            result = []
            
            for item in data:
                if key in item:
                    value = item[key]
                    if value not in seen_values:
                        seen_values.add(value)
                        result.append(item)
            
            return result
        
        elif fuzzy:
            # Fuzzy deduplication
            result = []
            
            for item in data:
                is_duplicate = False
                for existing in result:
                    if self._calculate_similarity(item, existing) > 0.8:
                        is_duplicate = True
                        break
                
                if not is_duplicate:
                    result.append(item)
            
            return result
        
        else:
            # Exact deduplication
            seen = set()
            result = []
            
            for item in data:
                item_str = json.dumps(item, sort_keys=True)
                if item_str not in seen:
                    seen.add(item_str)
                    result.append(item)
            
            return result
    
    def _calculate_similarity(self, item1: Dict, item2: Dict) -> float:
        """Calculate similarity between two data items"""
        # Simple similarity based on common fields and values
        common_fields = set(item1.keys()) & set(item2.keys())
        total_fields = set(item1.keys()) | set(item2.keys())
        
        if not total_fields:
            return 1.0
        
        field_similarity = len(common_fields) / len(total_fields)
        
        # Value similarity for common fields
        value_matches = 0
        for field in common_fields:
            if item1[field] == item2[field]:
                value_matches += 1
        
        value_similarity = value_matches / len(common_fields) if common_fields else 0
        
        return (field_similarity + value_similarity) / 2

class IntegratedOrganizationSystem:
    """Integrated organization system combining file and data organization"""
    
    def __init__(self, base_directory: str = None):
        self.file_organizer = FileOrganizer(base_directory)
        self.data_organizer = DataOrganizer()
        self.operation_history = []
    
    def organize_system(self, config: Dict) -> Dict[str, Any]:
        """Comprehensive system organization"""
        results = {
            "file_organization": {},
            "data_organization": {},
            "summary": {}
        }
        
        # File organization
        if "files" in config:
            file_config = config["files"]
            
            if file_config.get("organize_by_type"):
                results["file_organization"]["by_type"] = self.file_organizer.organize_by_type(
                    dry_run=file_config.get("dry_run", False)
                )
            
            if file_config.get("organize_by_date"):
                results["file_organization"]["by_date"] = self.file_organizer.organize_by_date(
                    date_format=file_config.get("date_format", "%Y/%m"),
                    dry_run=file_config.get("dry_run", False)
                )
            
            if file_config.get("deduplicate"):
                results["file_organization"]["deduplication"] = self.file_organizer.deduplicate_and_organize(
                    strategy=file_config.get("deduplication_strategy", "move")
                )
        
        # Data organization
        if "data" in config:
            data_config = config["data"]
            
            for data_source, operations in data_config.items():
                # Load data (placeholder - would need actual data loading logic)
                data = []  # Would load from file, database, etc.
                
                for operation in operations:
                    op_type = operation.get("type")
                    op_params = operation.get("params", {})
                    
                    if op_type == "group_by":
                        organized_data = self.data_organizer.organize_data(
                            data, "group_by", **op_params
                        )
                    elif op_type == "sort_by":
                        organized_data = self.data_organizer.organize_data(
                            data, "sort_by", **op_params
                        )
                    elif op_type == "filter_by":
                        organized_data = self.data_organizer.organize_data(
                            data, "filter_by", **op_params
                        )
                    else:
                        continue
                    
                    if data_source not in results["data_organization"]:
                        results["data_organization"][data_source] = []
                    
                    results["data_organization"][data_source].append({
                        "operation": op_type,
                        "params": op_params,
                        "result_count": len(organized_data) if isinstance(organized_data, list) else len(organized_data)
                    })
        
        # Summary
        results["summary"] = {
            "total_file_operations": sum(
                op.get("files_processed", 0) 
                for ops in results["file_organization"].values() 
                if isinstance(ops, dict)
            ),
            "total_data_operations": sum(
                len(ops) 
                for ops in results["data_organization"].values()
            )
        }
        
        # Record operation
        self.operation_history.append({
            "timestamp": time.time(),
            "config": config,
            "results": results
        })
        
        return results
    
    def get_system_stats(self) -> Dict[str, Any]:
        """Get comprehensive system statistics"""
        return {
            "file_organizer": self.file_organizer.get_organization_stats(),
            "data_organizer": {},  # Data organizer doesn't have persistent stats
            "operation_count": len(self.operation_history)
        }

if __name__ == "__main__":
    # Example usage
    organizer = IntegratedOrganizationSystem("/tmp/test_organization")
    
    # Add organization rule
    organizer.file_organizer.add_organization_rule({
        "name": "organize_images",
        "pattern": "*.jpg",
        "destination": "images/{year}/{month}",
        "condition": {"size_min": 1024}
    })
    
    # Organization configuration
    config = {
        "files": {
            "organize_by_type": True,
            "organize_by_date": True,
            "deduplicate": True,
            "dry_run": True
        },
        "data": {
            "sample_data": [
                {"type": "group_by", "params": {"key": "category"}},
                {"type": "sort_by", "params": {"key": "name"}}
            ]
        }
    }
    
    # Run organization
    results = organizer.organize_system(config)
    print("Organization Results:", json.dumps(results, indent=2))
    
    # Get stats
    stats = organizer.get_system_stats()
    print("System Stats:", json.dumps(stats, indent=2))
