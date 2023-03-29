import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse


#class MyOptions(PipelineOptions):
#  @classmethod
#  def _add_argparse_args(cls, parser):
#    parser.add_argument(
#        '--input',
#        default='gs://dataflow-samples/shakespeare/kinglear.txt',
#        help='The file path for the input text to process.')
#    parser.add_argument(
#        '--output', required=True, help='The path prefix for output files.')

class ComputeWordLengthFn(beam.DoFn):
  def process(self, element):
    return [[element,len(element)]]


def main():
    parser = argparse.ArgumentParser(description="Nuestro primer pipeline")
    parser.add_argument(
        "--entrada",
        default="./data/lorem.txt",
        help="Fichero de entrada"
        )
    parser.add_argument(
        "--salida",
        default= "./out/salida.txt",
        help="Fichero de salida"
        )
    #parser.add_argument(
    #    "--n_palabras",
    #    type=int,
    #    default = 10,
    #    help="Número de palabras en la salida")

    our_args, beam_args = parser.parse_known_args()
    run_pipeline(our_args, beam_args)


def palabras_largas(par_palabra):
    return par_palabra[1] > 13

def run_pipeline(custom_args, beam_args):
    entrada = custom_args.entrada
    salida = custom_args.salida
    #n_palabras = custom_args.n_palabras    
    opts = PipelineOptions(beam_args)

    with beam.Pipeline(options=opts) as pipeline:
        longitud_palabras = (
            pipeline
            | 'Leer archivo' >> beam.io.ReadFromText(entrada)
            | 'Separar en palabras' >> beam.FlatMap(lambda l:l.split(' '))
            | 'Calcular longitud de palabras' >> beam.ParDo(ComputeWordLengthFn())
            #| 'Filtrar palabras largas con lambda' >> beam.Filter(lambda l: l[1]> 13 )
            | 'Filtrar palabras largas con funcion' >> beam.Filter(palabras_largas)
            | 'Imprimir en pantalla' >> beam.Map(print)
        )
        
if __name__ == '__main__':
    main()
