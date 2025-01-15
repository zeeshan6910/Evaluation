import os
import json
import pandas as pd
from PyPDF2 import PdfReader
from multiprocessing import Pool

def extract_text_from_pdf(pdf_path):
    """Extract text from a PDF file."""
    reader = PdfReader(pdf_path)
    text = ""
    for page in reader.pages:
        text += page.extract_text() + "\n"
    return text.strip()

def mapper(pdf_path):
    """Mapper function to process a single PDF file."""
    try:
        text = extract_text_from_pdf(pdf_path)
        return {"file_name": os.path.basename(pdf_path), "content": text}
    except Exception as e:
        print(f"Error processing {pdf_path}: {e}")
        return None

def reducer(mapped_data):
    """Reducer function to consolidate mapped data."""
    return [data for data in mapped_data if data is not None]

def process_pdf_directory_mapreduce(input_folder, output_file, num_workers=4):
    """Process all PDFs in a directory and save the extracted data to a JSON file."""
    pdf_files = [os.path.join(input_folder, filename) for filename in os.listdir(input_folder) if filename.endswith('.pdf')]

    with Pool(num_workers) as pool:
        mapped_data = pool.map(mapper, pdf_files)

    reduced_data = reducer(mapped_data)

    # Save data to a JSON file
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(reduced_data, f, ensure_ascii=False, indent=4)

    print(f"Data saved to {output_file}")

def convert_json_to_csv(json_file, csv_file):
    """Convert the JSON to CSV"""
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Convert JSON data to a DataFrame and save as CSV
    df = pd.DataFrame(data)
    df.to_csv(csv_file, index=False)

    print(f"Data saved to {csv_file}")


if __name__ == "__main__":
    # Define paths
    input_folder = "./data-processing/pdfs"  # PDF files
    output_json = "output_data_map_reduce.json"
    output_csv = "output_data_map_reduce.csv"

    # Extract and process PDFs
    process_pdf_directory_mapreduce(input_folder, output_json)

    # Convert JSON to CSV
    convert_json_to_csv(output_json, output_csv)
