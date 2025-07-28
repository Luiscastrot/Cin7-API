from http.server import BaseHTTPRequestHandler, HTTPServer
import urllib.parse as urlparse

PORT = 8080

class OAuthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed_path = urlparse.urlparse(self.path)
        query_params = urlparse.parse_qs(parsed_path.query)

        if 'code' in query_params:
            code = query_params['code'][0]
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(f"<html><body><h1>Authorization successful!</h1><p>Copy this code:</p><pre>{code}</pre></body></html>".encode('utf-8'))
            print(f"Authorization code received: {code}")
        else:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"Error: Authorization code not found in the request.")

def run_server():
    print(f"Starting server at http://localhost:{PORT}")
    server_address = ('', PORT)
    httpd = HTTPServer(server_address, OAuthHandler)
    httpd.serve_forever()

if __name__ == '__main__':
    run_server()
