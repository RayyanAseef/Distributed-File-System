
from client.service import FileService


# Displays the available operations
def display_menu():
    print("\n===== Distributed File Storage System =====")
    print("1. Upload File")
    print("2. Download File")
    print("3. List Files")
    print("4. Exit")


# Starts the command-line interface
def main():

    service = FileService()

    while True:

        display_menu()
        choice = input("\nSelect an option: ").strip()

        if choice == "1":

            file_path = input("Enter file path: ").strip()

            try:
                result = service.upload(file_path)

                print(f"\n{result['message']}")
                print(f"File: {result['filename']}")
                print(f"Size: {result['size']} bytes")
                print(f"Chunks: {result['chunks']}")

            except (OSError, ValueError, RuntimeError) as error:
                print(f"\nUpload failed: {error}")

        elif choice == "2":

            filename = input("Enter filename: ").strip()

            destination = input(
                "Enter destination path (press Enter for default): "
            ).strip()

            if not destination:
                destination = None

            try:
                result = service.download(filename, destination)

                print(f"\n{result['message']}")
                print(f"Saved to: {result['path']}")

            except (OSError, ValueError, RuntimeError) as error:
                print(f"\nDownload failed: {error}")

        elif choice == "3":

            try:
                files = service.list_files()

                if not files:
                    print("\nNo files currently stored.")

                else:
                    print("\nStored Files:")

                    for index, filename in enumerate(files, start=1):
                        print(f"{index}. {filename}")

            except (OSError, ValueError, RuntimeError) as error:
                print(f"\nFailed to retrieve files: {error}")

        elif choice == "4":
            print("\nExiting Distributed File Storage System.")
            break

        else:
            print("\nInvalid option. Please select 1-4.")


if __name__ == "__main__":
    main()
