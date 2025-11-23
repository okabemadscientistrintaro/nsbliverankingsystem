from flask import Flask, render_template, request, jsonify, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
from flask_socketio import SocketIO, emit, join_room
from datetime import datetime


app = Flask(__name__)
DATABASE_URL = 'postgresql+psycopg2://neondb_owner:npg_XTVb48QSFkPz@ep-dark-silence-adah8o3x-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require'
app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SECRET_KEY'] = 'a_very_secret_and_unique_key_for_socketio' 
app.config['SQLALCHEMY_POOL_RECYCLE'] = 600 
app.config['SQLALCHEMY_POOL_TIMEOUT'] = 10
db = SQLAlchemy(app)
socketio = SocketIO(app)


class Team(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    tournament_id = db.Column(db.String(10), nullable=False)
    league_level = db.Column(db.String(10), nullable=False)
    scores = db.relationship('Score', backref='team', lazy=True)

    def __repr__(self):
        return f"<Team {self.name} T{self.tournament_id} {self.league_level}>"

class Score(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    team_id = db.Column(db.Integer, db.ForeignKey('team.id'), nullable=False)
    tour_number = db.Column(db.Integer, nullable=False)
    score = db.Column(db.Float, nullable=False, default=0.0)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<Тур T{self.tour_number} Команда:{self.team_id} БАллы:{self.score}>"


def calculate_ranking(tournament_id, league_level):
    teams = Team.query.filter_by(
        tournament_id=tournament_id, 
        league_level=league_level
    ).all()

    ranking_list = []
    
    for team in teams:
        tour_scores = {i: 0.0 for i in range(1, 6)}
        total_score = 0.0

        for score_entry in team.scores:
            tour_number = score_entry.tour_number
            if 1 <= tour_number <= 5:
                tour_scores[tour_number] = score_entry.score
                total_score += score_entry.score
        
        team_data = {
            'team_name': team.name,
            'total_score': total_score,
            'tour_scores': tour_scores, 
            'team_id': team.id
        }
        ranking_list.append(team_data)

    ranking_list.sort(key=lambda x: x['total_score'], reverse=True)
    
    for i, team in enumerate(ranking_list):
        team['rank'] = i + 1
        
    return ranking_list

def broadcast_ranking(tournament_id, league_level):

    ranking_data = calculate_ranking(tournament_id, league_level)
    channel_name = f'ranking_{tournament_id}_{league_level}'
    
    socketio.emit('ranking_update', ranking_data, room=channel_name)
    print(f"Обновление отправлено в канал: {channel_name}")


@socketio.on('connect_to_ranking')
def handle_connect_to_ranking(data):

    tournament_id = data.get('tournament_id')
    league_level = data.get('league_level')
    
    if tournament_id and league_level:
        channel_name = f'ranking_{tournament_id}_{league_level}'
        join_room(channel_name)
        
        print(f"Клиент присоединился к комнате: {channel_name}")
        
        ranking_data = calculate_ranking(tournament_id, league_level)
        emit('ranking_update', ranking_data, room=request.sid)



@app.route('/')
def index():
    tournaments = ["APhB", "NChB", "AMB"] 
    leagues = ['Senior', 'Junior']
    return render_template('index.html', tournaments=tournaments, leagues=leagues)


@app.route('/ranking/<string:tournament_id>/<string:league_level>')
def show_ranking(tournament_id, league_level):
    display_level = league_level.capitalize()
    title = f"{tournament_id}: {display_level} Лига"
    return render_template('ranking_template.html', 
                           title=title,
                           tournament_id=tournament_id, 
                           league_level=display_level)

@app.route('/jury_input', methods=['GET', 'POST'])
# app.py

@app.route('/jury_input', methods=['GET', 'POST'])
def jury_input():
    if request.method == 'POST':
        data = request.json
        team_id = data.get('team_id')
        tour_number = data.get('tour_number')
        score_value = data.get('score')
        
        if team_id is None or tour_number is None or score_value is None:
            return jsonify({"success": False, "message": "Не хватает обязательных полей"}), 400
        
        try:
            team_id = int(team_id)
            tour_number = int(tour_number)
            score_value = float(score_value)
        except (TypeError, ValueError):
            return jsonify({"success": False, "message": "Неверный формат чисел для ID/тура/балла"}), 400

        team = Team.query.get(team_id)
        if not team or not (1 <= tour_number <= 5) or score_value is None:
            return jsonify({"success": False, "message": "Неверные данные (команда, тур или балл)"}), 400

        existing_score = Score.query.filter_by(
            team_id=team_id, 
            tour_number=tour_number
        ).first()
        
        if existing_score:
            existing_score.score = score_value
        else:
            new_score = Score(
                team_id=team_id, 
                tour_number=tour_number, 
                score=score_value
            )
            db.session.add(new_score)
            
        db.session.commit()
        
        broadcast_ranking(team.tournament_id, team.league_level)
        
        return jsonify({"success": True, "message": "Баллы обновлены!"})
        
    all_teams = Team.query.all()
    return render_template('jury_input.html', teams=all_teams)


with app.app_context():
    db.create_all()
    
    if not Team.query.all():
        print("Создание тестовых команд...")
        db.session.add_all([
            Team(name="Alpha Senior", tournament_id="APhB", league_level="Senior"),
            Team(name="Beta Senior", tournament_id="APhB", league_level="Senior"),
            Team(name="Gamma Junior", tournament_id="APhB", league_level="Junior"),
            Team(name="Delta Junior", tournament_id="APhB", league_level="Junior"),
            Team(name="T2 Team 1", tournament_id="APhB", league_level="Senior"),
        ])
        db.session.commit()
        print("Тестовые команды созданы. Зайдите в /jury_input для ввода баллов.")

if __name__ == '__main__':
    socketio.run(app, debug=True)