package sleet.id;

public abstract class BaseIdError implements IdError {
  private final String message; 

  public BaseIdError(String message) {
    this.message = message;
  }
  
  @Override
  public String getErrorMessage() {
    return this.message;
  }

}
