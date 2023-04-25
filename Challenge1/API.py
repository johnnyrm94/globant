from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy
import datetime

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'postgresql://postgres:Nasus1994.@globantdb.c1fhwpqnf58i.us-east-1.rds.amazonaws.com:5432/globantdb'
db = SQLAlchemy(app)

class Departments(db.Model):
    __tablename__ = 'departments'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    department = db.Column(db.String(50), nullable=False)

    def to_dict(self):
        return {
            'id': self.id,
            'department': self.department
        }

class Jobs(db.Model):
    __tablename__ = 'jobs'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    job = db.Column(db.String(50), nullable=False)

    def to_dict(self):
        return {
            'id': self.id,
            'job': self.job
        }

class HiredEmployees(db.Model):
    __tablename__ = 'hired_employees'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    name = db.Column(db.String(50), nullable=False)
    datetime = db.Column(db.DateTime, nullable=False)
    department_id = db.Column(db.Integer, db.ForeignKey('departments.id'), nullable=False)
    job_id = db.Column(db.Integer, db.ForeignKey('jobs.id'), nullable=False)

    department = db.relationship('Departments', backref=db.backref('employees', lazy=True))
    job = db.relationship('Jobs', backref=db.backref('employees', lazy=True))

    def to_dict(self):
        return {
            'id': self.id,
            'name': self.name,
            'datetime': datetime.datetime.strptime(self.datetime, '%Y-%m-%dT%H:%M:%SZ').isoformat() if self.datetime else None,
            'department_id': self.department_id,
            'job_id': self.job_id
        }


@app.route('/employees', methods=['POST']) #Insert employees
def create_employees():
    data = request.json
    employees = data.get('employees')

    if not employees:
        return jsonify({'error': 'No employees provided'}), 400
    
    for employee in employees:
        name = employee.get('name')
        datetime_str = employee.get('datetime')
        department_id = employee.get('department_id')
        job_id = employee.get('job_id')
        table_name = employee.get('table')

        if not all([name, datetime_str, department_id, job_id, table_name]):
            return jsonify({'error': 'Missing data for employee'}), 400

        try:
            datetime_obj = datetime.datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%SZ')
        except ValueError as e:
            return jsonify({'error': 'Invalid datetime format. Expected format is YYYY-MM-DDTHH:MM:SSZ.'}), 400

        if table_name == 'Departments':
            table = Departments
        elif table_name == 'Jobs':
            table = Jobs
        elif table_name == 'HiredEmployees':
            table = HiredEmployees
        else:
            return jsonify({'error': f'Invalid table name: {table_name}'}), 400

        department = Departments.query.get_or_404(department_id)
        job = Jobs.query.get_or_404(job_id)

        employee = table(name=name, datetime=datetime_obj, department=department, job=job)
        db.session.add(employee)

    db.session.commit()
    return jsonify({'success': True}), 201

@app.route('/employees', methods=['GET'])
def get_employees():
    table_name = request.args.get('table')
    if table_name:
        if table_name == 'Departments':
            employees = Departments.query.all()
        elif table_name == 'Jobs':
            employees = Jobs.query.all()
        elif table_name == 'HiredEmployees':
            employees = HiredEmployees.query.all()
        else:
            return jsonify({'error': f'Invalid table name: {table_name}'}), 400
    else:
        employees = []
        for table in [Departments, Jobs, HiredEmployees]:
            employees += table.query.all()

    return jsonify([employee.to_dict() for employee in employees])

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(debug=True)
