package example;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;

public class Home implements Serializable,Comparator<Home>{ 

    private double price = Math.random() * 1000; 
    
    String name;

public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}



public Home(double price, String name) {
		super();
		this.price = price;
		this.name = name;
	}

public double getPrice() {
		return price;
	}

	public void setPrice(double price) {
		this.price = price;
	}

/*@Override
public int compareTo(Home o) {
    return Double.compare(this.getPrice(),o.getPrice());
}*/

@Override
public int compare(Home o1, Home o2) {
	// TODO Auto-generated method stub
	
	return Double.compare(o1.getPrice(),o2.getPrice());
}

@Override
public int hashCode() {
	final int prime = 31;
	int result = 1;
	result = prime * result + ((name == null) ? 0 : name.hashCode());
	long temp;
	temp = Double.doubleToLongBits(price);
	result = prime * result + (int) (temp ^ (temp >>> 32));
	return result;
}

@Override
public boolean equals(Object obj) {
	if (this == obj)
		return true;
	if (obj == null)
		return false;
	if (getClass() != obj.getClass())
		return false;
	Home other = (Home) obj;
	if (name == null) {
		if (other.name != null)
			return false;
	} else if (!name.equals(other.name))
		return false;
	if (Double.doubleToLongBits(price) != Double.doubleToLongBits(other.price))
		return false;
	return true;
}


}

