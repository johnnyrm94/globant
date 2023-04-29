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
            datetime_obj = datetime.datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%SZ').isoformat()
        except ValueError as e:
            return jsonify({'error': 'Invalid datetime format. Expected format is YYYY-MM-DDTHH:MM:SSZ.'}), 400

        if table_name == 'HiredEmployees':
            table = HiredEmployees
        else:
            return jsonify({'error': f'Invalid table name: {table_name}'}), 400

        department = Departments.query.get_or_404(department_id)
        job = Jobs.query.get_or_404(job_id)

        employee = table(name=name, datetime=datetime_obj, department=department, job=job)
        db.session.add(employee)

    db.session.commit()
    return jsonify({'success': True}), 201

@app.route('/departments', methods=['POST']) #Insert departments
def create_departments():
    data = request.json
    departments = data.get('departments')

    if not departments:
        return jsonify({'error': 'No departments provided'}), 400
    
    for department in departments:
        id = department.get('id')
        department = department.get('department')
        table_name = department.get('table')

        if not all([id, department]):
            return jsonify({'error': 'Missing data for department'}), 400

        if table_name == 'Departments':
            table = Departments

        else:
            return jsonify({'error': f'Invalid table name: {table_name}'}), 400
        

        department = table(id=id, department=department)
        db.session.add(department)

    db.session.commit()
    return jsonify({'success': True}), 201

@app.route('/jobs', methods=['POST']) #Insert jobs
def create_jobs():
    data = request.json
    jobs = data.get('jobs')

    if not jobs:
        return jsonify({'error': 'No jobs provided'}), 400
    
    for job in jobs:
        id = job.get('id')
        job = job.get('job')
        table_name = job.get('table')

        if not all([id, job]):
            return jsonify({'error': 'Missing data for job'}), 400

        
        if table_name == 'Jobs':
            table = Jobs

        else:
            return jsonify({'error': f'Invalid table name: {table_name}'}), 400

        job = table(id=id, job=job)
        db.session.add(job)

    db.session.commit()
    return jsonify({'success': True}), 201


@app.route('/employees', methods=['GET'])
def get_employees():
    table_name = request.args.get('table')
    if table_name:
        if table_name == 'HiredEmployees':
            employees = HiredEmployees.query.all()
        else:
            return jsonify({'error': f'Invalid table name: {table_name}'}), 400
    else:
        employees = []
        for table in [HiredEmployees]:
            employees += table.query.all()

    return jsonify([employee.to_dict() for employee in employees])



@app.route('/departments', methods=['GET'])
def get_departments():
    table_name = request.args.get('table')
    if table_name:
        if table_name == 'Departments':
            departments = Departments.query.all()
        else:
            return jsonify({'error': f'Invalid table name: {table_name}'}), 400
    else:
        departments = []
        for table in [Departments]:
            departments += table.query.all()

    return jsonify([department.to_dict() for department in departments])

@app.route('/jobs', methods=['GET'])
def get_jobs():
    table_name = request.args.get('table')
    if table_name:
        if table_name == 'Jobs':
            jobs = Jobs.query.all()
        else:
            return jsonify({'error': f'Invalid table name: {table_name}'}), 400
    else:
        jobs = []
        for table in [ Jobs]:
            jobs += table.query.all()

    return jsonify([job.to_dict() for job in jobs])

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    app.run(debug=True)


#PENDING: 
# ADD ISO FORMAT TO GET EMPLOYEEES
# ADD VALIDATION LIKE NOT INSERT MORE THAN 100 AND OTHER RULES. 
# DOCKER 
# PUT DOCKER ON CLOUD 
