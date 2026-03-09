import functions_framework

@functions_framework.http
def main_handler(request):
    return "Smoke Test Success", 200
