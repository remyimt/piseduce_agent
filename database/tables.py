from database.base import Base
from sqlalchemy import Boolean, Column, DateTime, Integer, String, Text


class Node(Base):
    __tablename__ = 'node'
    name = Column(Text, primary_key=True)
    status = Column(Text)


    def __repr__(self):
        return "Node(%s, %s)" % (self.name, self.status)


class NodeProperty(Base):
    __tablename__ = 'node_prop'
    uid = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(Text)
    prop_name = Column(Text)
    prop_value = Column(Text)


class Switch(Base):
    __tablename__ = 'switch'
    uid = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(Text)
    prop_name = Column(Text)
    prop_value = Column(Text)


class Environment(Base):
    __tablename__ = 'environment'
    uid = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(Text)
    prop_name = Column(Text)
    prop_value = Column(Text)


class Action(Base):
    __tablename__ = 'action'
    uid = Column(Integer, primary_key=True, autoincrement=True)
    node_name = Column(Text)
    node_ip = Column(Text)
    environment = Column(Text)
    process = Column(Text)
    state = Column(String(120))
    state_idx = Column(Integer)
    updated_at = Column(DateTime)


    def __repr__(self):
        return "action(%s, %s, %s)" % (self.node_name, self.state, self.updated_at)


class ActionProperty(Base):
    __tablename__ = 'action_prop'
    uid = Column(Integer, primary_key=True, autoincrement=True)
    node_name = Column(Text)
    prop_name = Column(Text)
    prop_value = Column(Text)
    owner = Column(Text)


class Schedule(Base):
    __tablename__ = 'schedule'
    name = Column(Text, primary_key=True)
    owner = Column(Text, primary_key=True)
    bin = Column(Text)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    status = Column(Text)
    action_state = Column(Text)


    def __repr__(self):
        return "schedule(%s, %s, %s)" % (self.name, self.status, self.start_date)
