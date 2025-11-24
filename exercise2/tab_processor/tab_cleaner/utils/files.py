import os


def check_file_exists(directory, filename):
    """Checks if a file exists in the specified directory."""
    file_path = os.path.join(directory, filename)
    return os.path.isfile(file_path)


def write_string_to_file(directory, file_name, text):
    """Writes a string to a file in the specified directory."""
    # Ensure the directory exists
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Create the full file path
    file_path = os.path.join(directory, file_name)

    # Write the string to the file
    with open(file_path, "w") as file:
        file.write(text)
