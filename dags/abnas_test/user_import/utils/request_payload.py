null = None


def row_data_for_input_file(item):
    return [
        item["Employee ID"].strip() if item["Employee ID"] else null,
        item["First Name"].strip() if item["First Name"] else null,
        item["Last Name"].strip() if item["Last Name"] else null,
        item["Preferred Name"].strip() if item["Preferred Name"] else null,
        item["Email"].strip() if item["Email"] else null
    ]