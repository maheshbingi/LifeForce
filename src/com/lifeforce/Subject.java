package com.lifeforce;

import java.util.ArrayList;
import java.util.List;

public interface Subject {
	List<Observer> observers = new ArrayList<Observer>();
	public void attachObserver(Observer observer);
	public void detachObserver(Observer observer);
	public void notifyObservers();
}
