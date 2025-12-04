from capgemini_test.time_export.config  import *
import rail

def get_uri_by_display_text(data, target_display_text):
    print(data)
    print(rail.result("print_test"))
    for item in data:
        if item.get('displayText') == target_display_text:
            return item.get('uri')
    return None