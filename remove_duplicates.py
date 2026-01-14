#!/usr/bin/env python3
"""
Remove Duplicates System - Organized duplicate removal utilities
"""

import re
import hashlib
import os
import time
import logging
from typing import List, Dict, Any, Set, Optional
from pathlib import Path
from collections import defaultdict

logger = logging.getLogger(__name__)

class DuplicateRemover:
    """Advanced duplicate removal system with multiple strategies"""
    
    def __init__(self):
        self.removed_count = 0
        self.strategies = {
            'exact': self._remove_exact_duplicates,
            'fuzzy': self._remove_fuzzy_duplicates,
            'semantic': self._remove_semantic_duplicates,
            'hash_based': self._remove_hash_based_duplicates
        }
    
    def remove_duplicates(self, items: List[Any], strategy: str = 'exact', **kwargs) -> List[Any]:
        """
        Remove duplicates from a list using specified strategy
        
        Args:
            items: List of items to deduplicate
            strategy: Deduplication strategy ('exact', 'fuzzy', 'semantic', 'hash_based')
            **kwargs: Strategy-specific parameters
        
        Returns:
            Deduplicated list preserving order
        """
        if strategy not in self.strategies:
            raise ValueError(f"Unknown strategy: {strategy}. Available: {list(self.strategies.keys())}")
        
        logger.info(f"🔄 Removing duplicates using {strategy} strategy")
        original_count = len(items)
        
        result = self.strategies[strategy](items, **kwargs)
        self.removed_count = original_count - len(result)
        
        logger.info(f"✅ Removed {self.removed_count} duplicates ({original_count} -> {len(result)})")
        return result
    
    def _remove_exact_duplicates(self, items: List[Any], **kwargs) -> List[Any]:
        """Remove exact duplicates while preserving order"""
        seen = set()
        result = []
        
        for item in items:
            if item not in seen:
                seen.add(item)
                result.append(item)
        
        return result
    
    def _remove_fuzzy_duplicates(self, items: List[str], threshold: float = 0.8, **kwargs) -> List[str]:
        """Remove fuzzy duplicates based on string similarity"""
        if not all(isinstance(item, str) for item in items):
            raise ValueError("Fuzzy deduplication requires string items")
        
        result = []
        
        for item in items:
            is_duplicate = False
            for existing in result:
                similarity = self._calculate_similarity(item, existing)
                if similarity >= threshold:
                    is_duplicate = True
                    break
            
            if not is_duplicate:
                result.append(item)
        
        return result
    
    def _remove_semantic_duplicates(self, items: List[str], **kwargs) -> List[str]:
        """Remove semantic duplicates (same meaning, different wording)"""
        if not all(isinstance(item, str) for item in items):
            raise ValueError("Semantic deduplication requires string items")
        
        # Normalize items by removing common variations
        normalized_items = []
        for item in items:
            normalized = self._normalize_text(item)
            normalized_items.append((item, normalized))
        
        # Remove duplicates based on normalized form
        seen_normalized = set()
        result = []
        
        for original, normalized in normalized_items:
            if normalized not in seen_normalized:
                seen_normalized.add(normalized)
                result.append(original)
        
        return result
    
    def _remove_hash_based_duplicates(self, items: List[Any], **kwargs) -> List[Any]:
        """Remove duplicates based on content hash"""
        seen_hashes = set()
        result = []
        
        for item in items:
            item_hash = self._calculate_hash(item)
            if item_hash not in seen_hashes:
                seen_hashes.add(item_hash)
                result.append(item)
        
        return result
    
    def _calculate_similarity(self, str1: str, str2: str) -> float:
        """Calculate similarity between two strings"""
        # Simple Levenshtein-like similarity
        longer = str1 if len(str1) > len(str2) else str2
        shorter = str2 if len(str1) > len(str2) else str1
        
        if len(longer) == 0:
            return 1.0
        
        edit_distance = self._levenshtein_distance(longer, shorter)
        return (len(longer) - edit_distance) / len(longer)
    
    def _levenshtein_distance(self, str1: str, str2: str) -> int:
        """Calculate Levenshtein distance between two strings"""
        if len(str1) < len(str2):
            return self._levenshtein_distance(str2, str1)
        
        if len(str2) == 0:
            return len(str1)
        
        previous_row = list(range(len(str2) + 1))
        for i, c1 in enumerate(str1):
            current_row = [i + 1]
            for j, c2 in enumerate(str2):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row
        
        return previous_row[-1]
    
    def _normalize_text(self, text: str) -> str:
        """Normalize text for semantic comparison"""
        # Convert to lowercase
        text = text.lower()
        
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        
        # Remove common punctuation
        text = re.sub(r'[^\w\s]', '', text)
        
        # Remove common prefixes/suffixes
        text = re.sub(r'^(the|a|an)\s+', '', text)
        text = re.sub(r'\s+(inc|corp|llc|ltd)\.?$', '', text)
        
        return text
    
    def _calculate_hash(self, item: Any) -> str:
        """Calculate hash for any item"""
        if isinstance(item, str):
            return hashlib.md5(item.encode()).hexdigest()
        elif isinstance(item, (dict, list)):
            return hashlib.md5(str(sorted(item.items()) if isinstance(item, dict) else str(item)).encode()).hexdigest()
        else:
            return hashlib.md5(str(item).encode()).hexdigest()

class FileDuplicateRemover(DuplicateRemover):
    """Specialized duplicate remover for files"""
    
    def __init__(self, directory: str = None):
        super().__init__()
        self.directory = directory
        self.file_hashes = {}
    
    def scan_directory(self, directory: str = None) -> Dict[str, List[str]]:
        """Scan directory for duplicate files"""
        target_dir = directory or self.directory
        if not target_dir:
            raise ValueError("No directory specified")
        
        logger.info(f"🔍 Scanning directory: {target_dir}")
        
        hash_map = defaultdict(list)
        
        for root, dirs, files in os.walk(target_dir):
            for file in files:
                file_path = os.path.join(root, file)
                try:
                    file_hash = self._calculate_file_hash(file_path)
                    hash_map[file_hash].append(file_path)
                except Exception as e:
                    logger.warning(f"⚠️  Could not hash {file_path}: {e}")
        
        # Filter to only duplicates
        duplicates = {hash_val: paths for hash_val, paths in hash_map.items() if len(paths) > 1}
        
        logger.info(f"📊 Found {len(duplicates)} sets of duplicate files")
        return dict(duplicates)
    
    def remove_file_duplicates(self, directory: str = None, keep_first: bool = True) -> Dict[str, Any]:
        """Remove duplicate files from directory"""
        duplicates = self.scan_directory(directory)
        
        removed_files = []
        kept_files = []
        
        for hash_val, file_paths in duplicates.items():
            if keep_first:
                files_to_remove = file_paths[1:]
                files_to_keep = [file_paths[0]]
            else:
                files_to_remove = file_paths[:-1]
                files_to_keep = [file_paths[-1]]
            
            for file_path in files_to_remove:
                try:
                    os.remove(file_path)
                    removed_files.append(file_path)
                    logger.info(f"🗑️  Removed duplicate: {file_path}")
                except Exception as e:
                    logger.error(f"💥 Failed to remove {file_path}: {e}")
            
            kept_files.extend(files_to_keep)
        
        return {
            'removed_files': removed_files,
            'kept_files': kept_files,
            'total_removed': len(removed_files),
            'total_kept': len(kept_files)
        }
    
    def _calculate_file_hash(self, file_path: str) -> str:
        """Calculate MD5 hash of file content"""
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            raise Exception(f"Failed to hash file {file_path}: {e}")

class ListDuplicateRemover(DuplicateRemover):
    """Specialized duplicate remover for lists with complex items"""
    
    def remove_dict_duplicates(self, items: List[Dict], key: str = None) -> List[Dict]:
        """Remove duplicates from list of dictionaries"""
        if key:
            # Remove duplicates based on specific key
            seen_values = set()
            result = []
            
            for item in items:
                if key in item and item[key] not in seen_values:
                    seen_values.add(item[key])
                    result.append(item)
            
            return result
        else:
            # Remove exact duplicates
            return self._remove_hash_based_duplicates(items)
    
    def remove_nested_duplicates(self, items: List[Any], depth: int = 3) -> List[Any]:
        """Remove duplicates from nested structures"""
        result = []
        
        for item in items:
            if isinstance(item, (list, dict)):
                # Recursively process nested structures
                if isinstance(item, list):
                    cleaned_item = self.remove_nested_duplicates(item, depth - 1) if depth > 0 else item
                else:  # dict
                    cleaned_item = {k: self.remove_nested_duplicates([v], depth - 1)[0] if depth > 0 and isinstance(v, (list, dict)) else v 
                                  for k, v in item.items()}
                
                # Check if this cleaned item is already in result
                if cleaned_item not in result:
                    result.append(cleaned_item)
            else:
                if item not in result:
                    result.append(item)
        
        return result

# Utility functions
def remove_duplicates_preserve_order(items: List[Any]) -> List[Any]:
    """Simple function to remove duplicates while preserving order"""
    seen = set()
    result = []
    
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    
    return result

def remove_duplicates_by_key(items: List[Dict], key: str) -> List[Dict]:
    """Remove duplicates from list of dictionaries by specific key"""
    seen_values = set()
    result = []
    
    for item in items:
        if key in item and item[key] not in seen_values:
            seen_values.add(item[key])
            result.append(item)
    
    return result

if __name__ == "__main__":
    # Example usage
    remover = DuplicateRemover()
    
    # Test with strings
    test_items = ["apple", "banana", "apple", "orange", "banana", "grape"]
    print("Original:", test_items)
    print("Deduplicated:", remover.remove_duplicates(test_items))
    print(f"Removed {remover.removed_count} duplicates")
    
    # Test with fuzzy matching
    fuzzy_items = ["Apple Inc.", "Apple Inc", "APPLE INC", "Apple Corporation"]
    print("\nFuzzy deduplication:")
    print("Original:", fuzzy_items)
    print("Deduplicated:", remover.remove_duplicates(fuzzy_items, strategy='fuzzy', threshold=0.7))
