from flask import Flask, render_template, request, jsonify, redirect, url_for
from flask_sqlalchemy import SQLAlchemy
from flask_socketio import SocketIO, emit, join_room
from datetime import datetime
import json
from sqlalchemy import Table, Column, Integer, Float, DateTime, text, select, update, insert

app = Flask(__name__)
# ВАШЕ ПОДКЛЮЧЕНИЕ К БД:
DATABASE_URL = 'postgresql+psycopg2://neondb_owner:npg_XTVb48QSFkPz@ep-dark-silence-adah8o3x-pooler.c-2.us-east-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require'
app.config['SQLALCHEMY_DATABASE_URI'] = DATABASE_URL
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SECRET_KEY'] = 'a_very_secret_and_unique_key_for_socketio'

# --- ИСПРАВЛЕНИЕ ОШИБКИ ПОДКЛЮЧЕНИЯ/ТАЙМ-АУТА ПУЛА ---
# SQLALCHEMY_POOL_PRE_PING = True гарантирует, что соединение тестируется перед использованием.
app.config['SQLALCHEMY_POOL_PRE_PING'] = True 
# Уменьшаем время переработки (recycle) до 300 секунд (5 минут), 
# чтобы избежать тайм-аута сервера БД, который часто составляет 5 минут.
app.config['SQLALCHEMY_POOL_RECYCLE'] = 300 
app.config['SQLALCHEMY_POOL_TIMEOUT'] = 10
# ------------------------------------------------------

db = SQLAlchemy(app)
socketio = SocketIO(app)

# ------------------------------------------------------------------
# ГЛОБАЛЬНЫЕ НАСТРОЙКИ
# ------------------------------------------------------------------
TOURNAMENTS = ["APhB", "NChB", "AMB"] 
LEAGUES = ['Senior', 'Junior']
ALL_COMBINATIONS = [(t, l) for t in TOURNAMENTS for l in LEAGUES]

# ------------------------------------------------------------------
# 1. ДАННЫЕ О КОМАНДАХ ИЗ JSON (Чтение из файлов)
# ------------------------------------------------------------------

def load_teams_from_json(tournament_id, league_level):
    """
    Загружает список команд, считывая соответствующий JSON-файл с диска.
    Файл должен называться: [tournament_id.lower()]_[league_level.lower()].json
    """
    key = f"{tournament_id.lower()}_{league_level.lower()}"
    filename = f"{key}.json"
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            teams = json.load(f)
            # Добавляем небольшую проверку, что это список
            if isinstance(teams, list):
                return teams
            else:
                print(f"Ошибка: Файл {filename} содержит не список.")
                return []
    except FileNotFoundError:
        # Это предупреждение в консоли, которое вы видели
        print(f"ВНИМАНИЕ: Файл данных команды не найден: {filename}")
        return []
    except json.JSONDecodeError:
        print(f"Ошибка: Неверный формат JSON в файле: {filename}")
        return []

# ------------------------------------------------------------------
# 2. CORE SQLAlchemy: Динамическое создание таблиц
# ------------------------------------------------------------------

def get_score_table(tournament_id, league_level):
    """
    Возвращает объект Table для нужной комбинации турнира/лиги.
    Создает его, если он еще не в метаданных.
    """
    table_name = f'score_{tournament_id}_{league_level}'
    
    if table_name not in db.metadata.tables:
        # Создаем колонки СВЕЖИМИ для каждой таблицы
        SCORE_TABLE_COLUMNS = [
            Column('id', Integer, primary_key=True),
            Column('team_id', Integer, nullable=False), # ID команды из JSON
            Column('tour_number', Integer, nullable=False),
            Column('score', Float, nullable=False, default=0.0),
            Column('timestamp', DateTime, default=datetime.utcnow)
        ]

        # Создаем объект Table, но пока не создаем саму таблицу в БД
        Table(table_name, db.metadata, *SCORE_TABLE_COLUMNS)
        print(f"Таблица {table_name} зарегистрирована в метаданных.")
        
    return db.metadata.tables[table_name]


def ensure_tables_exist():
    """Создает все 6 таблиц в БД, если они не существуют."""
    print("Проверка и создание 6 таблиц Score...")
    for t_id, l_level in ALL_COMBINATIONS:
        get_score_table(t_id, l_level) # Регистрируем все 6 таблиц
        
    # Создаем все зарегистрированные таблицы в БД
    db.create_all()
    print("Все 6 таблиц Score готовы.")
    
# ------------------------------------------------------------------
# 3. ЛОГИКА РАСЧЕТА РЕЙТИНГА
# ------------------------------------------------------------------

def calculate_ranking(tournament_id, league_level):
    """Рассчитывает рейтинг, используя команды из JSON и баллы из динамической таблицы."""
    
    # Теперь читаем команды из файла
    teams_json = load_teams_from_json(tournament_id, league_level)
    score_table = get_score_table(tournament_id, league_level)

    # 1. Получаем список ID команд для фильтрации запроса
    team_ids = [team['id'] for team in teams_json]
    
    if not team_ids:
        return []

    # 2. Эффективно загружаем все баллы, относящиеся к этим командам, используя Core SQL
    stmt = select(score_table.c.team_id, score_table.c.tour_number, score_table.c.score).where(
        score_table.c.team_id.in_(team_ids)
    )
    
    result = db.session.execute(stmt).all()
    
    # Возвращаем соединение в пул сразу после чтения
    db.session.remove()
    
    # 3. Инициализируем карту для подсчета
    scores_map = {}
    for team_id in team_ids:
        scores_map[team_id] = {'tour_scores': {i: 0.0 for i in range(1, 6)}, 'total_score': 0.0}

    # 4. Заполняем карту баллами из БД
    for t_id, t_num, score_val in result:
        if t_id in scores_map and 1 <= t_num <= 5:
            # Обновляем балл, только если он есть в БД. Это перебивает дефолт 0.0
            scores_map[t_id]['tour_scores'][t_num] = score_val
            scores_map[t_id]['total_score'] += score_val

    # 5. Формируем финальный список рейтинга
    ranking_list = []
    
    for team_data in teams_json:
        team_id = team_data['id']
        scores = scores_map.get(team_id, {'tour_scores': {i: 0.0 for i in range(1, 6)}, 'total_score': 0.0}) # Защита
        
        ranking_list.append({
            'team_name': team_data['name'],
            'total_score': scores['total_score'],
            'tour_scores': scores['tour_scores'],
            'team_id': team_id,
            'tournament_id': tournament_id,
            'league_level': league_level
        })
    
    # 6. Сортировка и присвоение места
    ranking_list.sort(key=lambda x: x['total_score'], reverse=True)
    
    for i, team in enumerate(ranking_list):
        team['rank'] = i + 1
        
    return ranking_list

def broadcast_ranking(tournament_id, league_level):
    """Отправляет обновленный рейтинг в нужный SocketIO канал."""
    ranking_data = calculate_ranking(tournament_id, league_level)
    channel_name = f'ranking_{tournament_id}_{league_level}'
    
    socketio.emit('ranking_update', ranking_data, room=channel_name)
    print(f"Обновление отправлено в канал: {channel_name}")


# ------------------------------------------------------------------
# 4. МАРШРУТЫ И SOCKETIO
# ------------------------------------------------------------------

@socketio.on('connect_to_ranking')
def handle_connect_to_ranking(data):
    """Обрабатывает подключение клиента к конкретному каналу рейтинга."""
    tournament_id = data.get('tournament_id')
    league_level = data.get('league_level')
    
    if tournament_id in TOURNAMENTS and league_level in LEAGUES:
        channel_name = f'ranking_{tournament_id}_{league_level}'
        join_room(channel_name)
        
        print(f"Клиент присоединился к комнате: {channel_name}")
        
        # calculate_ranking будет вызван, и db.session.remove() очистит соединение
        ranking_data = calculate_ranking(tournament_id, league_level)
        emit('ranking_update', ranking_data, room=request.sid)

@app.route('/')
def index():
    """Перенаправляет базовый URL на дашборд со всеми рейтингами."""
    return redirect(url_for('all_rankings_dashboard'))


@app.route('/ranking/<string:tournament_id>/<string:league_level>')
def show_ranking(tournament_id, league_level):
    """Показывает рейтинг для одного турнира/лиги."""
    display_level = league_level.capitalize()
    title = f"{tournament_id}: {display_level} Лига"
    return render_template('ranking_template.html', 
                           title=title,
                           tournament_id=tournament_id, 
                           league_level=league_level)


@app.route('/all_rankings')
def all_rankings_dashboard():
    """Собирает данные для всех 6 рейтингов для дашборда."""
    
    dashboard_data = {}
    
    for tour_id, level in ALL_COMBINATIONS:
        key = f'{tour_id}_{level}'
        ranking = calculate_ranking(tour_id, level)
        dashboard_data[key] = {
            'title': f'{tour_id} ({level})',
            'data': ranking,
            'tournament_id': tour_id,
            'league_level': level
        }
            
    return render_template('all_rankings_dashboard.html', 
                           data=dashboard_data, 
                           tournaments=TOURNAMENTS,
                           leagues=LEAGUES)


@app.route('/get_teams/<string:tournament_id>/<string:league_level>')
def get_teams(tournament_id, league_level):
    """Возвращает команды для AJAX-запроса, загружая их из JSON."""
    
    # Теперь данные загружаются из файлов
    teams = load_teams_from_json(tournament_id, league_level)
    return jsonify(teams)


@app.route('/jury_input', methods=['GET', 'POST'])
def jury_input():
    """Обработка ввода баллов жюри."""
    if request.method == 'POST':
        data = request.json
        team_id = data.get('team_id')
        tour_number = data.get('tour_number')
        score_value = data.get('score')
        
        # 1. Валидация
        if team_id is None or tour_number is None or score_value is None:
            return jsonify({"success": False, "message": "Не хватает обязательных полей"}), 400
        
        try:
            team_id = int(team_id)
            tour_number = int(tour_number)
            score_value = float(score_value)
        except (TypeError, ValueError):
            return jsonify({"success": False, "message": "Неверный формат чисел для ID/тура/балла"}), 400

        if not (1 <= tour_number <= 5):
            return jsonify({"success": False, "message": "Неверный номер тура"}), 400

        # 2. Находим турнир/лигу по ID команды (поиск по JSON-данным)
        found_team = None
        tour_broadcast = None
        level_broadcast = None
        
        for tour_id, level in ALL_COMBINATIONS:
            # Читаем команды из файла
            teams = load_teams_from_json(tour_id, level)
            for team in teams:
                if team['id'] == team_id:
                    found_team = team
                    tour_broadcast = tour_id
                    level_broadcast = level
                    break
            if found_team:
                break
                
        if not found_team:
            return jsonify({"success": False, "message": "Команда с таким ID не найдена в данных"}), 404
        
        score_table = get_score_table(tour_broadcast, level_broadcast)
        
        # 3. Проверка существования записи (Core SQL)
        check_stmt = select(score_table).where(
            score_table.c.team_id == team_id,
            score_table.c.tour_number == tour_number
        )
        existing_record = db.session.execute(check_stmt).first()
        
        record_id = None
        if existing_record:
            # Обновление (UPDATE)
            update_stmt = update(score_table).where(score_table.c.id == existing_record[0]).values(
                score=score_value,
                timestamp=datetime.utcnow()
            )
            db.session.execute(update_stmt)
            message = "Баллы обновлены!"
        else:
            # Вставка (INSERT)
            insert_stmt = insert(score_table).values(
                team_id=team_id, 
                tour_number=tour_number, 
                score=score_value
            )
            db.session.execute(insert_stmt)
            message = "Баллы добавлены!"
            
        db.session.commit()
        
        # Возвращаем соединение в пул сразу после записи
        db.session.remove()
        
        # 4. Отправка обновления
        broadcast_ranking(tour_broadcast, level_broadcast)
        
        return jsonify({"success": True, "message": message})
        
    # GET-запрос: Загрузка формы
    return render_template('jury_input.html')


# ------------------------------------------------------------------
# 5. ЗАПУСК ПРИЛОЖЕНИЯ
# ------------------------------------------------------------------

with app.app_context():
    # Создаем все 6 таблиц при старте, если они не существуют
    ensure_tables_exist()
    print("Внимание: Команды загружаются из отдельных JSON-файлов.")
    print("6 таблиц Score готовы. Зайдите в /jury_input для ввода баллов.")

if __name__ == '__main__':
    socketio.run(app, debug=True)