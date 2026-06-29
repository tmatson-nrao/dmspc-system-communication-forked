class RobotsMiddleware:
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        response = self.get_response(request)
        
        # Injects a global HTTP header telling all bots not to index the response
        response['X-Robots-Tag'] = 'noindex, nofollow'
        
        return response
