import bcrypt
# Fix passlib incompatibility with bcrypt 4.0+
if not hasattr(bcrypt, "__about__"):
    class BcryptAbout:
        __version__ = getattr(bcrypt, "__version__", "4.0.0")
    bcrypt.__about__ = BcryptAbout()

from passlib.context import CryptContext

pwd_cxt = CryptContext(schemes=["bcrypt"], deprecated="auto")


class Hash:
    @staticmethod
    def bcrypt(password: str):
        return pwd_cxt.hash(password)

    @staticmethod
    def verify(hashed_password, plain_password):
        return pwd_cxt.verify(plain_password, hashed_password)
    
    @staticmethod
    def checkpw(plain_password: str, hashed_password: str):
        """Compatibility method for existing code that expects checkpw"""
        return pwd_cxt.verify(plain_password, hashed_password)