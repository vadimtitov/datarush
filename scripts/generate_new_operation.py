#!/usr/bin/env python3
import sys
import re
from pathlib import Path

def snake_to_camel(name: str) -> str:
    return ''.join(word.capitalize() for word in name.split('_'))

def insert_sorted_unique(lines, new_item):
    clean = sorted(set(line.strip().rstrip(",") for line in lines if line.strip()))
    if new_item not in clean:
        clean.append(new_item)
    return sorted(set(clean))

def format_block(lines, indent="    "):
    return ''.join(f"{indent}{line},\n" for line in sorted(set(lines)))

def fix_init_formatting(code: str) -> str:
    # 1. Remove commas from section headers
    code = re.sub(r"# (Source|Sink|Transformation),", r"# \1", code)

    # 2. Ensure all entries in registration block have exactly one comma
    def fix_registration_block(match):
        lines = match.group(1).strip().splitlines()
        fixed = []
        for line in lines:
            line = line.strip().rstrip(",")
            if line and not line.startswith("#"):
                line = f"    {line},"
            fixed.append(line)
        return "for _op_type in [\n" + "\n".join(fixed) + "\n]:"

    code = re.sub(r"for _op_type in \[\n(.*?)\n\]:", fix_registration_block, code, flags=re.DOTALL)

    # 3. Clean up import blocks and fix double commas
    def fix_import_block(match):
        header, body, tail = match.groups()
        lines = [line.strip().rstrip(",") for line in body.strip().splitlines()]
        lines = sorted(set(lines))
        return header + ''.join(f"    {line},\n" for line in lines) + tail

    code = re.sub(
        r"(from datarush\.core\.operations\.\w+ import \(\n)(.*?)(\))",
        fix_import_block,
        code,
        flags=re.DOTALL
    )

    # 4. Remove accidental double commas anywhere
    code = re.sub(r",,+", ",", code)

    return code

def main():
    if len(sys.argv) != 3:
        print("Usage: add_operation.py <operation_name> <operation_type>")
        sys.exit(1)

    operation_name = sys.argv[1]
    operation_type = sys.argv[2].lower()
    valid_types = ["source", "sink", "transformation"]

    if operation_type not in valid_types:
        print(f"Invalid operation_type. Choose from: {', '.join(valid_types)}")
        sys.exit(1)

    class_name = snake_to_camel(operation_name)

    # 1. Create operation source file
    op_path = Path(f"src/datarush/core/operations/{operation_type}s/{operation_name}.py")
    op_path.parent.mkdir(parents=True, exist_ok=True)
    if not op_path.exists():
        op_path.write_text(f"class {class_name}:\n    title = \"{class_name}\"\n    name = \"{operation_name}\"\n")

    # 2. Create test file
    test_path = Path(f"tests/unit/operations/test_{operation_name}.py")
    test_path.parent.mkdir(parents=True, exist_ok=True)
    if not test_path.exists():
        test_path.write_text(f"def test_{operation_name}():\n    assert True\n")

    # 3. Update __init__.py
    init_path = Path("src/datarush/core/operations/__init__.py")
    code = init_path.read_text()

    ### 3a. Update import block
    import_pattern = rf"(from datarush\.core\.operations\.{operation_type}s import \(\n)(.*?)(\))"
    match = re.search(import_pattern, code, re.DOTALL)
    if not match:
        print(f"❌ Could not find import block for {operation_type}s.")
        sys.exit(1)

    before, middle, after = match.groups()
    imports = [line.strip().rstrip(",") for line in middle.strip().splitlines()]
    updated_imports = insert_sorted_unique(imports, operation_name)
    import_block = before + ''.join(f"    {line},\n" for line in updated_imports) + after
    code = re.sub(import_pattern, import_block, code, flags=re.DOTALL)

    ### 3b. Update registration block
    reg_pattern = r"(for _op_type in \[\n)(.*?)(\n\]:)"
    match = re.search(reg_pattern, code, re.DOTALL)
    if not match:
        print("❌ Could not find operation registration list.")
        sys.exit(1)

    reg_before, reg_body, reg_after = match.groups()
    lines = reg_body.strip().splitlines()

    new_entry = f"{operation_name}.{class_name}"
    section_header = f"# {operation_type.capitalize()}"
    idx = next((i for i, l in enumerate(lines) if section_header in l), -1)

    if idx == -1:
        print(f"❌ Could not find marker '# {operation_type.capitalize()}' in registration list.")
        sys.exit(1)

    entries = [l.strip().rstrip(",") for l in lines if l.strip() and not l.strip().startswith("#")]
    if new_entry not in entries:
        lines.insert(idx + 1, f"    {new_entry},")

    reg_block = reg_before + "\n".join(lines) + reg_after
    code = re.sub(reg_pattern, reg_block, code, flags=re.DOTALL)

    # 4. Final formatting clean-up
    code = fix_init_formatting(code)

    # 5. Save
    init_path.write_text(code)
    print(f"✅ Operation '{operation_name}' added with fully cleaned __init__.py.")

if __name__ == "__main__":
    main()
