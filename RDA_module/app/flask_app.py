import logging
from wage_trust_rda import RDA


from flask import Flask, jsonify, request
from six.moves import http_client

app = Flask(__name__)

## Change inline with Xuns dictionary
@app.route('/test_endpoint', methods=['GET', 'POST'])
def placeholder():
    if request.method == 'GET':
        return jsonify({
            'message'   : 'GET works'
        })

    elif request.method == 'POST':
        return jsonify({
            'message'   : 'POST works',
            'payload'   : request.json
        })






@app.route('/rda_test_1', methods=['POST'])
def test_results_1():
    if request.method == 'POST':
        rda = RDA()
    return jsonify(rda.test_1())

# @app.route('/rda_test_2', methods=['POST'])
# def test_results_2():
#     if request.method == 'POST':
#         rda = RDA()
#     return jsonify(rda.test_2())

@app.route('/rda_test_3', methods=['POST'])
def test_results_3():
    if request.method == 'POST':
        rda = RDA()
    return jsonify(rda.test_3())

@app.route('/rda_test_4', methods=['POST'])
def test_results_4():
    if request.method == 'POST':
        rda = RDA()
    return jsonify(rda.test_4())

# @app.route('/rda_test_5', methods=['POST'])
# def test_results_5():
#     if request.method == 'POST':
#         rda = RDA()
#     return jsonify(rda.test_5())

@app.route('/rda_working_pattern', methods=['POST'])
def test_results_6():
    if request.method == 'POST':
        rda = RDA()
    return jsonify(rda.test_6())

@app.route('/rda_test_7', methods=['POST'])
def test_results_7():
    if request.method == 'POST':
        rda = RDA()
        # shift_duration_hrs is the the number of hours an employee must work to be entitled to a break
        # min_break_duration_mins is the minimum break an employee must have when breaching shift_duration_hrs
    return jsonify(rda.test_7(shift_duration_hrs=6.0, min_break_duration_mins=30.0))

@app.route('/rda_engagement_period', methods=['POST'])
def test_results_8():
    if request.method == 'POST':
        rda = RDA()
    return jsonify(rda.test_8())

@app.route('/rda_test_9', methods=['POST'])
def test_results_9():
    if request.method == 'POST':
        rda = RDA()
        # min_gap is a variable given the minimum number of hours between shifts needed. This can be changes here
    return jsonify(rda.test_9(min_gap=10.0))

@app.route('/rda_test_10', methods=['POST'])
def test_results_10():
    if request.method == 'POST':
        rda = RDA()
    return jsonify(rda.test_10())

# @app.route('/rda_working_pattern', methods=['GET'])
# def test_results():
#     if request.method == 'GET':
#         test_number = request.json.get('test_number')
#
#         rda = RDA()
#         test_dict = {
#             # "test_1": rda.test_1(),
#             # "test_2": rda.test_2(),
#             # "test_3": rda.test_3(),
#             # "test_4": rda.test_4(),
#             # "test_5": rda.test_5(),
#             # "test_7": rda.test_9(),
#             # "test_9": rda.test_9(),
#             # "test_10": rda.test_10()
#         }
#
#         test_results = test_dict[test_number]
#
#     return jsonify(test_results)

@app.errorhandler(http_client.INTERNAL_SERVER_ERROR)
def unexpected_error(e):
    '''
    Handle exceptions by returning swagger-compliant json.
    '''
    logging.exception('An error occured while processing the request.')

    response = jsonify({
        'code'      : http_client.INTERNAL_SERVER_ERROR,
        'message'   : 'the request failed'
        })

    response.status_code = http_client.INTERNAL_SERVER_ERROR

    return response


@app.errorhandler(404)
def pageNotFound(error):
    return '404\'d'
