package com.isikun.firat.totallyorderedmulticast;

import com.google.gson.Gson;

import java.io.Serializable;
import java.util.InvalidPropertiesFormatException;

/**
 * Created by hexenoid on 12/30/14.
 */
public class TOMOperation implements Serializable{

    public static final int ADDITION = 1;
    public static final int SUBSTRACTION = 2;
    public static final int DIVISION = 3;
    public static final int MULTIPLICATION = 4;

    private double value;
    private int operator;

    // expects operation followed by number
    public TOMOperation(String operation)throws InvalidPropertiesFormatException {
        switch (operation.charAt(0)){
            case '+':
                this.operator = 1;
                break;
            case '-':
                this.operator = 2;
                break;
            case '/':
                this.operator = 3;
                break;
            case '*':
                this.operator = 4;
                break;
            default:
                throw new InvalidPropertiesFormatException("Undefined Operator");
        }
        this.value = Double.parseDouble(operation.charAt(1) + "");
    }

    public TOMOperation(double value, String operator) throws InvalidPropertiesFormatException {
        switch (operator){
            case "+":
                this.operator = 1;
                break;
            case "-":
                this.operator = 2;
                break;
            case "/":
                this.operator = 3;
                break;
            case "*":
                this.operator = 4;
                break;
            default:
                throw new InvalidPropertiesFormatException("Undefined Operator");
        }
        this.value = value;
    }

    public double operate(double current){
        double result = current;
        switch(operator){
            case 1:
                result += value;
                break;
            case 2:
                result -= value;
                break;
            case 3:
                result /= value;
                break;
            case 4:
                result *= value;
                break;
            default:
                break;
        }
        return result;
    }

    public String toString(){
        return this.serialize();
    }

    public String serialize() {
        Gson gson = new Gson();
        String json = gson.toJson(this);
        return json;
    }

    public static TOMOperation deserialize(String message) {
        Gson gson = new Gson();
        return gson.fromJson(message, TOMOperation.class);
    }
}
