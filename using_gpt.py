from azure.ai.formrecognizer import FormRecognizerClient
from azure.core.credentials import AzureKeyCredential


def extract_tables_from_document(endpoint, key):
    # Initialize a Form Recognizer client
    form_recognizer_client = FormRecognizerClient(endpoint, AzureKeyCredential(key))

    # Submit the document for analysis
    with open(r"C:\Users\SESA737860\Desktop\azure\testpdf.pdf", "rb") as f:
        poller = form_recognizer_client.begin_recognize_content(form=f)
    # poller = form_recognizer_client.begin_recognize_tables_from_url(document_url)
    tables = poller.result()

    print(tables)
    # Extract tables from the result
    extracted_tables = []
    for idx, table in enumerate(tables):
        extracted_table = {
            "table_number": idx + 1,
            "rows": []
        }
        for row in table.rows:
            row_content = []
            for cell in row.cells:
                row_content.append(cell.text.strip())
            extracted_table["rows"].append(row_content)
        extracted_tables.append(extracted_table)

    return extracted_tables

# Example usage
endpoint = "https://textext.cognitiveservices.azure.com/"
key = ""
# document_url = "https://<your-storage-account-name>.blob.core.windows.net/<your-container-name>/<your-document-name>.pdf"
tables = extract_tables_from_document(endpoint, key)
print("Extracted Tables:")
for table in tables:
    print("Table {}: ".format(table["table_number"]))
    for row in table["rows"]:
        print(row)
