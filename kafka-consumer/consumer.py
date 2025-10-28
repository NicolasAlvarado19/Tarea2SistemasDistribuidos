import json
import logging
import os
import time
from kafka import KafkaConsumer, KafkaProducer
import google.generativeai as genai
from google.api_core import exceptions as google_exceptions

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class LLMConsumer:
    
    def __init__(self):
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
        self.gemini_api_key = os.getenv('GEMINI_API_KEY')
        self.gemini_model = os.getenv('GEMINI_MODEL', 'gemini-1.5-flash')
        
        # Configurar Gemini
        logger.info("Configurando cliente de Gemini...")
        genai.configure(api_key=self.gemini_api_key)
        self.model = genai.GenerativeModel(self.gemini_model)
        logger.info(f"✅ Gemini configurado: {self.gemini_model}")
        
        # Consumer de Kafka
        logger.info("Conectando consumer a Kafka...")
        self.consumer = KafkaConsumer(
            'preguntas-nuevas',
            bootstrap_servers=self.kafka_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='llm-consumer-group',
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        logger.info("✅ Consumer conectado")
        
        # Producer de Kafka
        logger.info("Conectando producer a Kafka...")
        self.producer = KafkaProducer(
            bootstrap_servers=self.kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=3
        )
        logger.info("✅ Producer conectado")
        
        # Métricas
        self.mensajes_procesados = 0
        self.respuestas_exitosas = 0
        self.errores_sobrecarga = 0
        self.errores_cuota = 0
        self.errores_otros = 0
    
    def llamar_llm(self, pregunta):
        """
        Llama al LLM con manejo de errores específicos
        """
        try:
            prompt = f"""Responde la siguiente pregunta de forma clara y concisa:

Pregunta: {pregunta}

Respuesta:"""
            
            response = self.model.generate_content(prompt)
            
            if response and response.text:
                return {
                    'exito': True,
                    'respuesta': response.text,
                    'error_tipo': None
                }
            else:
                return {
                    'exito': False,
                    'respuesta': None,
                    'error_tipo': 'sin_respuesta'
                }
        
        except google_exceptions.ResourceExhausted as e:
            # Error de cuota (rate limit)
            logger.warning(f"⚠️ CUOTA EXCEDIDA: {str(e)}")
            return {
                'exito': False,
                'respuesta': None,
                'error_tipo': 'cuota',
                'error_mensaje': str(e)
            }
        
        except google_exceptions.ServiceUnavailable as e:
            # Servicio no disponible / sobrecarga
            logger.warning(f"⚠️ SERVICIO NO DISPONIBLE: {str(e)}")
            return {
                'exito': False,
                'respuesta': None,
                'error_tipo': 'sobrecarga',
                'error_mensaje': str(e)
            }
        
        except google_exceptions.InternalServerError as e:
            # Error interno del servidor (sobrecarga)
            logger.warning(f"⚠️ ERROR INTERNO DEL SERVIDOR: {str(e)}")
            return {
                'exito': False,
                'respuesta': None,
                'error_tipo': 'sobrecarga',
                'error_mensaje': str(e)
            }
        
        except Exception as e:
            # Otros errores
            logger.error(f"❌ ERROR INESPERADO: {str(e)}")
            return {
                'exito': False,
                'respuesta': None,
                'error_tipo': 'desconocido',
                'error_mensaje': str(e)
            }
    
    def procesar_mensaje(self, mensaje):
        """
        Procesa un mensaje del topic preguntas-nuevas
        """
        pregunta = mensaje.get('pregunta')
        respuesta_original = mensaje.get('respuesta_original')
        intentos = mensaje.get('intentos', 0)
        
        logger.info(f"🔄 Procesando: {pregunta[:50]}... (Intento {intentos + 1})")
        
        # Llamar al LLM
        resultado = self.llamar_llm(pregunta)
        
        if resultado['exito']:
            # ✅ Respuesta exitosa
            logger.info("✅ Respuesta generada exitosamente")
            self.respuestas_exitosas += 1
            
            mensaje_exitoso = {
                'pregunta': pregunta,
                'respuesta_original': respuesta_original,
                'respuesta_llm': resultado['respuesta'],
                'timestamp': time.time(),
                'intentos': intentos + 1
            }
            
            self.producer.send('respuestas-exitosas', value=mensaje_exitoso)
            logger.info("📤 Enviado a topic: respuestas-exitosas")
        
        else:
            # ❌ Error - clasificar y enviar al topic correspondiente
            error_tipo = resultado['error_tipo']
            
            if error_tipo == 'cuota':
                logger.warning("📮 Enviando a topic: errores-cuota")
                self.errores_cuota += 1
                topic_destino = 'errores-cuota'
            
            elif error_tipo == 'sobrecarga':
                logger.warning("📮 Enviando a topic: errores-sobrecarga")
                self.errores_sobrecarga += 1
                topic_destino = 'errores-sobrecarga'
            
            else:
                logger.error("📮 Error desconocido - descartando mensaje")
                self.errores_otros += 1
                return  # No reintentamos errores desconocidos
            
            # Preparar mensaje para reintento
            mensaje_error = {
                'pregunta': pregunta,
                'respuesta_original': respuesta_original,
                'timestamp': time.time(),
                'intentos': intentos + 1,
                'error_tipo': error_tipo,
                'error_mensaje': resultado.get('error_mensaje', '')
            }
            
            self.producer.send(topic_destino, value=mensaje_error)
    
    def mostrar_metricas(self):
        """
        Muestra métricas del consumer
        """
        logger.info("="*80)
        logger.info(f"📊 MÉTRICAS DEL CONSUMER")
        logger.info("="*80)
        logger.info(f"Mensajes procesados: {self.mensajes_procesados}")
        logger.info(f"✅ Respuestas exitosas: {self.respuestas_exitosas}")
        logger.info(f"⚠️ Errores de cuota: {self.errores_cuota}")
        logger.info(f"⚠️ Errores de sobrecarga: {self.errores_sobrecarga}")
        logger.info(f"❌ Otros errores: {self.errores_otros}")
        
        if self.mensajes_procesados > 0:
            tasa_exito = (self.respuestas_exitosas / self.mensajes_procesados) * 100
            logger.info(f"📈 Tasa de éxito: {tasa_exito:.1f}%")
        
        logger.info("="*80)
    
    def ejecutar(self):
        """
        Loop principal del consumer
        """
        logger.info("="*80)
        logger.info("🚀 CONSUMER DE LLM INICIADO")
        logger.info("="*80)
        logger.info("📥 Esperando mensajes del topic: preguntas-nuevas")
        logger.info("="*80)
        
        try:
            for mensaje in self.consumer:
                self.mensajes_procesados += 1
                
                try:
                    self.procesar_mensaje(mensaje.value)
                    
                    # Mostrar métricas cada 10 mensajes
                    if self.mensajes_procesados % 10 == 0:
                        self.mostrar_metricas()
                
                except Exception as e:
                    logger.error(f"❌ Error al procesar mensaje: {str(e)}")
                    continue
        
        except KeyboardInterrupt:
            logger.info("\n⚠️ Deteniendo consumer...")
            self.mostrar_metricas()
        
        finally:
            self.consumer.close()
            self.producer.close()
            logger.info("✅ Consumer cerrado correctamente")

if __name__ == "__main__":
    consumer = LLMConsumer()
    consumer.ejecutar()