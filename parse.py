#!/usr/bin/env python3
"""
Log parser script to extract ConflictGraphSolver timing information.

This script parses log files to extract GlobalCommit timing phases from
trace-level logs.

Usage:
    python3 parse.py                    # Parse all log.*.txt files in temp/ directory
    python3 parse.py temp/log.0.txt     # Parse specific file
    python3 parse.py temp/log.*.txt     # Parse files matching pattern
    python3 parse.py file1.txt file2.txt # Parse multiple specific files

Algorithm:
1. Scan the log file for GlobalCommit trace messages
2. Treat the "Conflict detection for epoch …" line as the start of a block
3. Collect the following phases for each block:
    - Constructing transaction chains
    - Processing transaction chains
    - Constructing read/write trackers
    - Building conflict graph
    - Finalizing conflict graph
    - Conflict detection (graph)
    - Conflict detection (epoch)
    - Applying non-conflicting transactions
    - Re-executing
    - Epoch commit took
4. Finalise the block when the "Epoch … commit took" timing is seen
5. Calculate and display average, min, max, and sample values for each phase

Output: Average timing for each operation category across all log files.
"""

import re
import os
import glob
from collections import defaultdict
from typing import List, Dict, Tuple, Optional


class LogParser:
    def __init__(self):
        # Categories we want to track
        self.categories = [
            "Constructing transaction chains",
            "Processing transaction chains",
            "Constructing read/write trackers",
            "Building conflict graph",
            "Finalizing conflict graph",
            "Conflict detection (graph)",
            "Conflict detection (epoch)",
            "Applying non-conflicting transactions",
            "Re-executing",
            "Epoch commit took"
        ]
        
        # Patterns to match
        # Combined ConflictGraph + GlobalCommit trace patterns
        self.phase_patterns = {
            "Constructing transaction chains": r"\[.*?\]trce: Node-1\[0\] Constructing transaction chains took (\d+(?:\.\d+)?) ms",
            "Processing transaction chains": r"\[.*?\]trce: Node-1\[0\] Processing transaction chains took (\d+(?:\.\d+)?) ms",
            "Constructing read/write trackers": r"\[.*?\]trce: Node-1\[0\] Constructing read/write trackers took (\d+(?:\.\d+)?) ms",
            "Building conflict graph": r"\[.*?\]trce: Node-1\[0\] Building conflict graph took (\d+(?:\.\d+)?) ms",
            "Finalizing conflict graph": r"\[.*?\]trce: Node-1\[0\] Finalizing conflict graph took (\d+(?:\.\d+)?) ms",
            "Conflict detection (graph)": r"\[.*?\]trce: Node-1\[0\] Conflict detection took (\d+(?:\.\d+)?) ms",
            "Conflict detection (epoch)": r"\[.*?\]trce: Node-1\[0\] \[GlobalCommit\] Conflict detection for epoch \d+ took (\d+(?:\.\d+)?) ms",
            "Applying non-conflicting transactions": r"\[.*?\]trce: Node-1\[0\] \[GlobalCommit\] Applying non-conflicting transactions for epoch \d+ took (\d+(?:\.\d+)?) ms",
            "Re-executing": r"\[.*?\]trce: Node-1\[0\] \[GlobalCommit\] Re-executing took (\d+(?:\.\d+)?) ms",
            "Epoch commit took": r"\[.*?\]trce: Node-1\[0\] \[GlobalCommit\] Epoch \d+ commit took (\d+(?:\.\d+)?) ms"
        }

    def parse_file(self, file_path: str) -> Dict[str, List[float]]:
        """Parse a single log file and extract timing data."""
        results = defaultdict(list)
        
        try:
            with open(file_path, 'r') as f:
                lines = f.readlines()
        except Exception as e:
            print(f"Error reading file {file_path}: {e}")
            return results
            
        current_block_data: Dict[str, float] = {}
        collecting = False
        
        for line in lines:
            line = line.strip()

            for category, pattern in self.phase_patterns.items():
                match = re.search(pattern, line)
                if not match:
                    continue

                timing = float(match.group(1))

                if category == self.categories[0]:
                    # Starting a new block; if previous block was complete, persist it
                    if collecting:
                        if len(current_block_data) == len(self.categories):
                            for cat, value in current_block_data.items():
                                results[cat].append(value)
                        else:
                            print("Incomplete GlobalCommit block encountered; skipping")
                    current_block_data = {}
                    collecting = True

                if collecting:
                    current_block_data[category] = timing

                    if category == self.categories[-1]:
                        # End of block
                        if len(current_block_data) == len(self.categories):
                            for cat, value in current_block_data.items():
                                results[cat].append(value)
                        else:
                            print("Incomplete GlobalCommit block encountered at end; skipping")
                        current_block_data = {}
                        collecting = False
                break
        
        # Flush final block if file ended without explicit close
        if collecting and current_block_data:
            if len(current_block_data) == len(self.categories):
                for cat, value in current_block_data.items():
                    results[cat].append(value)
            else:
                print("Incomplete GlobalCommit block encountered at EOF; skipping")
        
        return results

    def parse_multiple_files(self, file_patterns: List[str]) -> Dict[str, List[float]]:
        """Parse multiple log files and aggregate results."""
        aggregated_results = defaultdict(list)
        
        all_files = set()  # Use set to avoid duplicates
        for pattern in file_patterns:
            files = glob.glob(pattern)
            all_files.update(files)
        
        all_files = sorted(list(all_files))  # Convert back to sorted list
        
        if not all_files:
            print("No files found matching the patterns")
            return aggregated_results
            
        print(f"Processing {len(all_files)} files: {all_files}")
        
        for file_path in all_files:
            print(f"\nProcessing {file_path}...")
            file_results = self.parse_file(file_path)
            
            # Aggregate results
            for category, timings in file_results.items():
                aggregated_results[category].extend(timings)
                print(f"  {category}: {len(timings)} measurements")
        
        return aggregated_results

    def calculate_averages(self, results: Dict[str, List[float]]) -> Dict[str, float]:
        """Calculate average times for each category."""
        averages = {}
        for category, timings in results.items():
            if timings:
                averages[category] = sum(timings) / len(timings)
            else:
                averages[category] = 0.0
        return averages

    def print_results(self, results: Dict[str, List[float]], averages: Dict[str, float]):
        """Print the results in a formatted way."""
        print("\n" + "="*80)
        print("TIMING ANALYSIS RESULTS")
        print("="*80)
        
        for category in self.categories:
            timings = results.get(category, [])
            avg = averages.get(category, 0.0)
            
            print(f"\n{category}:")
            print(f"  Count: {len(timings)}")
            print(f"  Average: {avg:.2f} ms")
            if timings:
                print(f"  Min: {min(timings)} ms")
                print(f"  Max: {max(timings)} ms")
                print(f"  Values: {timings[:10]}{'...' if len(timings) > 10 else ''}")
        
        print("\n" + "="*80)
        print("SUMMARY - Average times:")
        for category in self.categories:
            avg = averages.get(category, 0.0)
            print(f"  {category}: {avg:.2f} ms")
        print("="*80)


def main():
    parser = LogParser()
    
    # Default pattern to look for log files in the temp directory
    default_patterns = [
        "temp/log.*.txt"
    ]
    
    # You can also specify custom file patterns
    import sys
    if len(sys.argv) > 1:
        file_patterns = sys.argv[1:]
    else:
        file_patterns = default_patterns
    
    print("Log Parser for ConflictGraphSolver Timing Analysis")
    print(f"Looking for files matching: {file_patterns}")
    
    # Parse all files
    results = parser.parse_multiple_files(file_patterns)
    
    if not results:
        print("No data found matching the specified patterns.")
        return
    
    # Calculate averages
    averages = parser.calculate_averages(results)
    
    # Print results
    parser.print_results(results, averages)


if __name__ == "__main__":
    main()
