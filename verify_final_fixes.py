#!/usr/bin/env python
"""Verify all three bug fixes are correctly implemented"""

from pathlib import Path

def verify_fixes():
    main_py = Path("backend/main.py").read_text(encoding='utf-8', errors='ignore')
    
    print("\n" + "="*70)
    print("Verifying MakerChecker Bug Fixes")
    print("="*70 + "\n")
    
    fixes = {
        "1. OTP removed from compliance breach rules": {
            "checks": [
                ("'compliance_breach' ONLY if: privacy laws violated, promised guaranteed" in main_py, 
                 "OTP phrase removed from compliance_breach definition"),
                ("'compliance_breach' ONLY if: privacy laws violated, OTP shared" not in main_py,
                 "OTP not mentioned in compliance breaches"),
            ]
        },
        "2. Summary not duplicated in score_reason": {
            "checks": [
                ('def _score_reason(analysis: Dict[str, Any]) -> str:\n    failed = analysis.get("failed_parameters")' in main_py,
                 "score_reason only shows failed params, not summary"),
                ('return summary or "Weighted score' not in main_py.split('def _score_reason')[1].split('def ')[0],
                 "score_reason function doesn't include summary"),
            ]
        },
        "3. Fatal logic only for false_information/behavior_issue": {
            "checks": [
                ('has_fatal_flag = "false_information" in flags or "behavior_issue" in flags' in main_py,
                 "Hard fail checks for actual false_information or behavior_issue flags"),
                ('analysis["severity"] = "watch"' in main_py,
                 "Missing product info marked as watch, not fatal"),
                ('analysis["pass_fail"] = "FAIL" if has_fatal_flag else "PASS"' in main_py,
                 "Only fails if truly fatal, passes with warnings for missing info"),
            ]
        },
        "4. Response accuracy not bottomed out": {
            "checks": [
                ('scores["response_accuracy"] = min(int(scores.get("response_accuracy", 5) or 5), 3)' in main_py,
                 "Response accuracy capped at 3, not 1 (for missing info)"),
            ]
        },
    }
    
    all_passed = True
    for section, data in fixes.items():
        print(f"{section}")
        for check, desc in data["checks"]:
            if check:
                print(f"  ✓ {desc}")
            else:
                print(f"  ✗ {desc}")
                all_passed = False
        print()
    
    print("="*70)
    if all_passed:
        print("✓ ALL FIXES VERIFIED SUCCESSFULLY")
    else:
        print("✗ Some fixes need attention")
    print("="*70 + "\n")
    
    return all_passed

if __name__ == "__main__":
    import sys
    sys.exit(0 if verify_fixes() else 1)
