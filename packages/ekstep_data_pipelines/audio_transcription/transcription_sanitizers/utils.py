def get_transcription_sanitizers(**kwargs):
    hindi_sanitizer = HindiSanitizer.get_instance(**kwargs)
    gujrati_sanitizer = GujratiSanitizer.get_instance(**kwargs)

    return {'hindi_sanitizer': hindi_sanitizer, 'gujrati': gujrati_sanitizer,'default': hindi_sanitizer}
