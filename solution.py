import os
import json
import pandas as pd
from PyPDF2 import PdfReader

def extract_text_from_pdf(pdf_path):
    """Extract text from a PDF file."""
    reader = PdfReader(pdf_path)
    text = ""
    for page in reader.pages:
        text += page.extract_text() + "\n"
    return text.strip()

def process_pdf_directory(input_folder, output_file):
    """Process all PDFs in a directory and save the extracted data to a JSON file."""
    structured_data = []

    for filename in os.listdir(input_folder):
        if filename.endswith('.pdf'):
            pdf_path = os.path.join(input_folder, filename)
            try:
                text = extract_text_from_pdf(pdf_path)
                structured_data.append({"file_name": filename, "content": text})
            except Exception as e:
                print(f"Error processing {filename}: {e}")

    # Save data to a JSON file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(structured_data, f, ensure_ascii=False, indent=4)

    print(f"Data saved to {output_file}")

if __name__ == "__main__":
    # Path for input pdf files
    input_folder = "./pdfs"  
    output_json = "output_data.json"
    
    # Extract and process PDFs
    process_pdf_directory(input_folder, output_json)

    