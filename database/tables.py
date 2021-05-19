from database.base import Base
from sqlalchemy import Boolean, Column, DateTime, Integer, String, Text


class Node(Base):
    __tablename__ = 'node'
    node_name = Column(Text, primary_key=True)
    prop_name = Column(Text, primary_key=True)
    prop_value = Column(Text)


    def __repr__(self):
        return "node(%s, %s, %s)" % (self.node_name, self.prop_name, self.prop_value)


class Switch(Base):
    __tablename__ = 'switch'
    name = Column(Text, primary_key=True)
    prop_name = Column(Text, primary_key=True)
    prop_value = Column(Text)


class Environment(Base):
    __tablename__ = 'environment'
    name = Column(Text, primary_key=True)
    prop_name = Column(Text, primary_key=True)
    prop_value = Column(Text)


class Action(Base):
    __tablename__ = 'action'
    node_name = Column(Text, primary_key=True)
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
    node_name = Column(Text, primary_key=True)
    prop_name = Column(Text, primary_key=True)
    prop_value = Column(Text)
    owner = Column(Text, primary_key=True)


class Schedule(Base):
    __tablename__ = 'schedule'
    node_name = Column(Text, primary_key=True)
    owner = Column(Text, primary_key=True)
    bin = Column(Text)
    start_date = Column(DateTime)
    end_date = Column(DateTime)
    state = Column(Text)
    action_state = Column(Text)


    def __repr__(self):
        return "schedule(%s, %s, %s)" % (self.node_name, self.state, self.start_date)
