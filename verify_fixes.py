#!/usr/bin/env python
"""
Simple verification of bug fixes without full imports
"""

import re
from pathlib import Path

def check_main_py_changes():
    """Check that all required changes are in main.py"""
    main_py = Path("backend/main.py").read_text(encoding='utf-8', errors='ignore')
    
    print("Checking main.py for all required fixes...\n")
    
    checks = {
        "1. Restricted flagging in SYSTEM_PROMPT": [
            ("DO NOT flag for: customer saying", "Negative examples in prompt"),
            ("DO NOT flag for: vague responses", "Restrictions on vague flagging"),
            ("compliance_breach", "compliance_breach flag mentioned"),
            ("false_information", "false_information flag mentioned"),
            ("regulatory_violation", "regulatory_violation flag mentioned"),
            ("behavior_issue", "behavior_issue flag mentioned"),
            ("ONLY flag if the bot actually violated", "Explicit requirement to flag only violations"),
        ],
        "2. Cache system implementation": [
            ("CACHE_DIR = PROC_DIR / \"cache\"", "Cache directory definition"),
            ("CACHE_EXPIRY_SECONDS = 14 * 24 * 60 * 60", "2-week cache expiry"),
            ("def get_cache(key: str)", "get_cache function"),
            ("def set_cache(key: str, value: Any)", "set_cache function"),
            ("def clear_expired_cache():", "clear_expired_cache function"),
            ("clear_expired_cache()", "Cache cleanup called on startup"),
        ],
        "3. Product detection improvements": [
            ("best_score > 0.08", "Lowered threshold to 0.08"),
            ("cache_key = f\"product_context:", "Product context caching"),
            ("PRODUCT DETECTION INSTRUCTIONS:", "Better product detection in user prompt"),
        ],
        "4. Duplicate flagging prevention": [
            ("list(dict.fromkeys(flags))", "Flag deduplication"),
            ("if \"false_information\" not in flags:", "Conditional flag addition"),
            ("if \"behavior_issue\" not in flags:", "Conditional behavior_issue flag"),
        ],
    }
    
    all_passed = True
    for section, patterns in checks.items():
        print(f"{section}")
        section_passed = True
        for pattern, desc in patterns:
            if pattern in main_py:
                print(f"  ✓ {desc}")
            else:
                print(f"  ✗ {desc} - NOT FOUND")
                section_passed = False
                all_passed = False
        print()
    
    return all_passed

def main():
    print("\n" + "="*70)
    print("MakerChecker Bug Fixes - Code Verification")
    print("="*70 + "\n")
    
    if check_main_py_changes():
        print("="*70)
        print("✓ ALL FIXES VERIFIED - Code changes are correctly in place!")
        print("="*70)
        return 0
    else:
        print("="*70)
        print("✗ Some fixes are missing - see details above")
        print("="*70)
        return 1

if __name__ == "__main__":
    import sys
    sys.exit(main())
