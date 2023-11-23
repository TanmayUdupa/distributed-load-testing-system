from flask import Flask, jsonify, request

app = Flask(__name__)

# Initialize counters for requests and responses
request_count = 0
response_count = 0

@app.route('/')
def test_endpoint():
    # Increment the request count upon receiving a request
    global request_count
    request_count += 1

    # Your actual processing logic here...

    # Simulate a response for demonstration purposes
    response_data = {"message": "Hello, this is the target server!"}
    
    # Increment the response count upon sending a response
    global response_count
    response_count += 1

    return jsonify(response_data)

@app.route('/metrics')
def metrics():
    # Return the current counts as JSON
    return jsonify({"requests_received": request_count, "responses_sent": response_count})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
