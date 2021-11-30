from flask import Flask, request, jsonify, make_response, abort, redirect
from flask import send_file, render_template
from db_controller import DBController


app = Flask(__name__, template_folder='/app/html_templates')
db = DBController.getInstance()


@app.route('/')
def index():
    return 'Stratio Data Centric'


@app.route('/setcookie/<user>', methods=['POST', 'GET'])
def setcookie(user):

    resp = make_response()
    resp.set_cookie('userID', user)

    return resp

@app.route('/getcookie')
def getcookie():
   name = request.cookies.get('userID')
   return '<h1>welcome ' + str(name) + '</h1>'


@app.route('/edit_record/<rec_id>', methods=['GET'])
def edit_record(rec_id):
   template = render_template("index.html", data=rec_id)
   return template


@app.route('/edit_record2/<rec_id>', methods=['GET'])
def edit_record2(rec_id):
    comments = {"1": "Checked and ok. No deviation", "2": "Time weighted rate of return Incorrect"}
    to_remove_bools = {"1": "false", "2": "false"}
    query = "UPDATE perf_spike_cal set comment='{}', to_remove='{}' where id={}".format(comments[rec_id],
                                                                                        to_remove_bools[rec_id],
                                                                                        rec_id)
    db.manual_sql(query)
    if rec_id == "1":
        return redirect("https://www.figma.com/proto/5YjolN3Gn25rWIkj7LIcIz/hsbc-discovery-book?node-id=103%3A799&viewport=171%2C144%2C0.18651863932609558&scaling=min-zoom&page-id=103%3A798&hotspot-hints=0&hide-ui=1")
    if rec_id == "2":
        return redirect("https://www.figma.com/proto/5YjolN3Gn25rWIkj7LIcIz/hsbc-discovery-book?node-id=185%3A160&viewport=291%2C377%2C0.06345906853675842&scaling=min-zoom&page-id=185%3A141&hotspot-hints=0&hide-ui=1")
    return redirect("https://www.figma.com/proto/5YjolN3Gn25rWIkj7LIcIz/hsbc-discovery-book?node-id=103%3A799&viewport=171%2C144%2C0.18651863932609558&scaling=min-zoom&page-id=103%3A798&hotspot-hints=0&hide-ui=1")


@app.route('/save_results', methods=['POST'])
def save_results():
    comment = request.form.get('comment')
    to_remove = request.form.get('to_remove')
    to_remove_bool = False
    if to_remove is not None and to_remove == "remove":
        to_remove_bool = True
    rec_id = request.form.get('rec_id')
    print(comment)
    print(to_remove)
    print(to_remove_bool)
    print(rec_id)
    query = "UPDATE perf_spike_cal set comment='{}', to_remove='{}' where id={}".format(comment,
                                                                                        str(to_remove_bool).lower(),
                                                                                        rec_id)
    print(query)
    db.manual_sql(query)
    return jsonify({"result": "Done"})


def main():
    app.run(host='0.0.0.0', port=5051, threaded=True)


if __name__ == '__main__':
    main()
