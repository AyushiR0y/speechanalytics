#!/usr/bin/env python
"""
Test file to verify the bug fixes for:
1. Over-flagging issues
2. Product detection
3. Cache system
"""

import sys
from pathlib import Path

# Add backend to path
sys.path.insert(0, str(Path(__file__).parent / "backend"))

def test_imports():
    """Test that all modules import without errors"""
    try:
        from main import (
            get_cache, set_cache, clear_expired_cache,
            search_product_rag, infer_product_context,
            _apply_qa_policy_rules, SYSTEM_PROMPT
        )
        print("✓ All imports successful")
        return True
    except Exception as e:
        print(f"✗ Import error: {e}")
        return False

def test_flagging_restrictions():
    """Test that SYSTEM_PROMPT has restricted flagging"""
    from main import SYSTEM_PROMPT
    
    # Check that the prompt restricts flags to actual violations
    checks = [
        ("compliance_breach" in SYSTEM_PROMPT, "Prompt mentions compliance_breach"),
        ("false_information" in SYSTEM_PROMPT, "Prompt mentions false_information"),
        ("regulatory_violation" in SYSTEM_PROMPT, "Prompt mentions regulatory_violation"),
        ("behavior_issue" in SYSTEM_PROMPT, "Prompt mentions behavior_issue"),
        ("DO NOT flag for: customer saying" in SYSTEM_PROMPT, "Prompt has negative examples"),
        ("DO NOT flag for: vague responses" in SYSTEM_PROMPT, "Prompt restricts vague flagging"),
    ]
    
    all_passed = True
    for check, desc in checks:
        if check:
            print(f"✓ {desc}")
        else:
            print(f"✗ {desc}")
            all_passed = False
    
    return all_passed

def test_cache_system():
    """Test that cache system is available"""
    try:
        from main import get_cache, set_cache, CACHE_DIR, CACHE_EXPIRY_SECONDS
        
        # Verify cache constants
        checks = [
            (CACHE_DIR.exists(), f"Cache directory exists: {CACHE_DIR}"),
            (CACHE_EXPIRY_SECONDS == 14 * 24 * 60 * 60, "Cache expiry is 2 weeks (1209600 seconds)"),
        ]
        
        all_passed = True
        for check, desc in checks:
            if check:
                print(f"✓ {desc}")
            else:
                print(f"✗ {desc}")
                all_passed = False
        
        # Test cache operations
        test_key = "test_key"
        test_value = {"test": "data"}
        set_cache(test_key, test_value)
        cached = get_cache(test_key)
        if cached == test_value:
            print(f"✓ Cache read/write operations work")
        else:
            print(f"✗ Cache read/write failed: expected {test_value}, got {cached}")
            all_passed = False
        
        return all_passed
    except Exception as e:
        print(f"✗ Cache system error: {e}")
        return False

def test_product_detection_threshold():
    """Test that product detection threshold has been lowered"""
    import inspect
    from main import infer_product_context
    
    source = inspect.getsource(infer_product_context)
    
    if "best_score > 0.08" in source:
        print("✓ Product detection threshold lowered to 0.08")
        return True
    elif "best_score > 0.15" in source:
        print("✗ Product detection threshold still at old value (0.15)")
        return False
    else:
        print("✗ Could not find threshold in source")
        return False

def test_duplicate_flagging_prevention():
    """Test that duplicate flagging prevention is in place"""
    import inspect
    from main import _apply_qa_policy_rules
    
    source = inspect.getsource(_apply_qa_policy_rules)
    
    # Check for deduplication at the end
    if "list(dict.fromkeys(flags))" in source:
        print("✓ Duplicate flag deduplication in place")
        return True
    else:
        print("✗ Duplicate flag deduplication missing")
        return False

def main():
    """Run all tests"""
    print("\n" + "="*60)
    print("MakerChecker Bug Fix Verification Tests")
    print("="*60 + "\n")
    
    results = {}
    
    print("1. Testing imports...")
    results["imports"] = test_imports()
    print()
    
    print("2. Testing flagging restrictions in SYSTEM_PROMPT...")
    results["flagging"] = test_flagging_restrictions()
    print()
    
    print("3. Testing cache system implementation...")
    results["cache"] = test_cache_system()
    print()
    
    print("4. Testing product detection threshold...")
    results["product_threshold"] = test_product_detection_threshold()
    print()
    
    print("5. Testing duplicate flagging prevention...")
    results["dedup"] = test_duplicate_flagging_prevention()
    print()
    
    print("="*60)
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    print(f"Results: {passed}/{total} test groups passed")
    print("="*60 + "\n")
    
    if passed == total:
        print("✓ All fixes verified successfully!")
        return 0
    else:
        print("✗ Some fixes need attention")
        return 1

if __name__ == "__main__":
    sys.exit(main())
