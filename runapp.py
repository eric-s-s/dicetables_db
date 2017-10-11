from flask import Flask, jsonify, render_template, request

from dicetables_db.theserver import StandIn

app = Flask(__name__)


@app.route('/_add_numbers')
def add_numbers():
    a = request.args.get('a', '', type=str)
    answer = {'a': 1}
    # answer = StandIn().get_response(a)
    return jsonify(answer)


@app.route('/')
def index():
    return render_template('index.html')


