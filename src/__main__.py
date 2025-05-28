from src.main_datetime import WWRApp, show_error

if __name__ == '__main__':
    try:
        WWRApp()
    except Exception as e:
        show_error(str(e))
