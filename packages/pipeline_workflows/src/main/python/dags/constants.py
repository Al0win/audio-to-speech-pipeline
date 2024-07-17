AZURE_TRANSLATION_API = "azure"
GOOGLE_TRANSLATION_API = "google"
DEFAULT_TRANSLATION_API = AZURE_TRANSLATION_API

def translate_text(text, target_language, api=DEFAULT_TRANSLATION_API):
    if api == AZURE_TRANSLATION_API:
        # Call Azure Translation API
        print(f"Translating '{text}' to {target_language} using Azure API.")
        # Azure API call logic goes here
    elif api == GOOGLE_TRANSLATION_API:
        # Call Google Translation API
        print(f"Translating '{text}' to {target_language} using Google API.")
        # Google API call logic goes here
    else:
        raise ValueError("Unsupported translation API.")

# Example usage
translate_text("Hello, world!", "es")  # Translates using the default (Azure)
translate_text("Hello, world!", "fr", api=GOOGLE_TRANSLATION_API)  # Translates using Google
