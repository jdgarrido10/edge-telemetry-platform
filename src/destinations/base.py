from abc import ABC, abstractmethod


class BaseDestination(ABC):
    """
    Contrato que todo destino de salida debe cumplir.
    El gateway solo habla con esta interfaz. No sabe qué hay detrás.

    PREGUNTA: ¿por qué usar una clase abstracta y no simplemente un protocolo (duck typing)?
    Escribe tu razonamiento antes de continuar.
    """

    @abstractmethod
    async def send(self, payload: dict) -> bool:
        """
        Envía un payload al destino.
        Devuelve True si tiene éxito, False o lanza excepción si falla.

        CONTRATO:
        - No debe bloquear el event loop
        - Debe ser idempotente si es posible (¿por qué importa esto?)
        - El caller decide qué hacer si devuelve False
        """
        ...

    @abstractmethod
    async def is_available(self) -> bool:
        """
        Comprueba si el destino está disponible sin intentar enviar.
        TODO: ¿cuándo llamarías a este método y cuándo no?
        """
        ...
