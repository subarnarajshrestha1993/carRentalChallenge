import re

def extract_invoice_details(invoice_str: str) -> dict:
    """
    Extracts product, amount, and date from an invoice string of the form:
    "Invoice for [PRODUCT_NAME] of amount [AMOUNT] USD issued on [DATE]."
    """
    pattern = r"Invoice for (.+?) of amount (\d+(?:\.\d+)?) USD issued on (\d{4}-\d{2}-\d{2})\."
    match = re.match(pattern, invoice_str)

    if not match:

        raise ValueError("Invoice string is not in the expected format.")

    product, amount_str, date = match.groups()

    amount = int(amount_str) if amount_str.isdigit() else float(amount_str)

    return {
        "product": product,
        "amount": amount,
        "date": date
    }

# Example usage
invoice = "Invoice for MacBook Pro of amount 1200 USD issued on 2023-04-15."
print(extract_invoice_details(invoice))
# Output: { "product": "MacBook Pro", "amount": 1200, "date": "2023-04-15" }
