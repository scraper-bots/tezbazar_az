# Phone Number Validation Rules

## 1. Basic Format Requirements
* Length: Exactly 9 digits total (no more, no less)
* Characters: Only numeric digits (0-9) allowed
* Format: Must be a continuous string without any separators
* Invalid characters: No spaces, hyphens, plus signs, parentheses, or any other non-numeric characters

## 2. Valid Prefix Requirements
* Must begin with one of these two-digit prefixes:
  - 10, 12
  - 50, 51, 55
  - 60, 70, 77
  - 99

## 3. Third Digit Restrictions
* Position 3 must NOT be:
  - 0 (zero)
  - 1 (one)
* Position 3 must be one of: 2, 3, 4, 5, 6, 7, 8, 9

## 4. Multiple Number Handling
* Each phone number must be on a new line
* Do not use any separators between numbers (no commas)
* Do not use arrays or lists
* Each number is validated separately

## Example Formats

### Valid Numbers:
```
504787463  (✓ prefix "50", third digit "4")
772856234  (✓ prefix "77", third digit "2")
995847632  (✓ prefix "99", third digit "5")
```

### Invalid Numbers:
```
501787463  (✗ third digit is "1")
504 787463 (✗ contains space)
504-787463 (✗ contains hyphen)
5047874633 (✗ too long - 10 digits)
50478746   (✗ too short - 8 digits)
527874633  (✗ invalid prefix "52")
+50478746  (✗ contains plus sign)
```

### Multiple Numbers Format

Correct:
```
504787463
777856234
995847632
```

Incorrect:
```
504787463, 777856234
504787463;777856234
[504787463, 777856234]
(504787463)(777856234)
```