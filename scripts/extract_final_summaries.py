import json
import re
import os

INPUT_FILE = "data/json/generated_summaries.json"
OUTPUT_FILE = "data/json/cleaned_summaries.json"

def extract_clean_summary(text, technique):
    if not text:
        return ""

    # Strategy 1: Look for "Final Summary" marker at the start of a line
    # We split the text by this marker. We take the *last* part to avoid capturing 
    # mentions of "Final Summary" that might appear in the "Prompt Design" phase of Meta Prompting.
    # Pattern explanation:
    # (?:^|\n)      -> Start of string or new line
    # \s*           -> Optional whitespace
    # (?:\*\*|#+)?  -> Optional Markdown bold (**) or headers (#)
    # Final Summary -> The literal text
    # (?:\*\*|:)?   -> Optional closing bold or colon
    # \s*           -> Optional whitespace
    parts = re.split(r'(?:^|\n)\s*(?:\*\*|#+\s*)?Final Summary(?:\*\*|:)?\s*', text, flags=re.IGNORECASE)
    
    if len(parts) > 1:
        # Found the marker, take the last part
        return parts[-1].strip()
    
    # Strategy 2: Technique-specific fallbacks if "Final Summary" tag is missing
    
    if technique == "Tree-of-Thought (ToT)":
        # Look for Step 3
        step3_parts = re.split(r'STEP 3: SYNTHESIZE.*?\n', text, flags=re.IGNORECASE)
        if len(step3_parts) > 1:
            return step3_parts[-1].strip()

    if technique == "Chain-of-Verification (CoVe)":
        # Look for Stage 4
        stage4_parts = re.split(r'STAGE 4: CORRECTED FINAL SUMMARY.*?\n', text, flags=re.IGNORECASE)
        if len(stage4_parts) > 1:
            return stage4_parts[-1].strip()

    if technique == "Meta Prompting":
        # Look for Stage 2
        stage2_parts = re.split(r'STAGE 2: EXECUTE.*?\n', text, flags=re.IGNORECASE)
        if len(stage2_parts) > 1:
            return stage2_parts[-1].strip()

    # Strategy 3: Chain-of-Density (CoD) Default
    if technique == "Chain-of-Density (CoD)":
        # If CoD didn't match the "Final Summary" tag in Strategy 1, 
        # it means the model output *only* the summary as requested. 
        # So we return the whole text.
        return text.strip()

    # Fallback for others: If we can't find a delimiter, return the whole text 
    # (though this implies the prompt instruction for labels was ignored by the model)
    return text.strip()

def main():
    if not os.path.exists(INPUT_FILE):
        print(f"Error: {INPUT_FILE} not found.")
        return

    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        data = json.load(f)

    print(f"Processing {len(data)} entries...")

    processed_count = 0
    for entry in data:
        raw_output = entry.get("actual_output", "")
        technique = entry.get("technique", "")
        
        clean_summary = extract_clean_summary(raw_output, technique)
        
        # Add the new attribute
        entry["summary"] = clean_summary
        processed_count += 1

    # Save to new file
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"Successfully processed {processed_count} entries.")
    print(f"Saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
