import apache_beam as beam
import logging
from apache_beam.options.pipeline_options import PipelineOptions

import argparse

def main():

    logging.getLogger().setLevel(logging.WARNING)

    parser = argparse.ArgumentParser(description= "New pipeline")
    parser.add_argument(
        "--input",
        help = "Fichero de entrada",
    )
    parser.add_argument(
        "--output",
        help = "Fichero de salida. En nuestro caso, puede ser un archivo .csv o dirigirse a una tabla en BigQuery"
    )

    our_args, beam_args = parser.parse_known_args()

    run_pipeline(our_args, beam_args)

def run_pipeline(custom_args, beam_args):
    entrada = custom_args.input
    salida = custom_args.output

    opts = PipelineOptions(beam_args)

    #with beam.Pipeline(options=opts) as p:
    #    (p
    #     | "Leer entrada" >> beam.io.ReadFromText(entrada)
    #     | "Separar cada entrada" >> beam.FlatMap(lambda l: l.split(","))
    #     | "Conteo 1" >> beam.combiners.Count.PerElement()
    #     | "Imprimir" >> beam.Map(print)
    #    )

    #with beam.Pipeline(options=opts) as p:
    #    (p
    #     | "Leer entrada" >> beam.io.ReadFromText(entrada)
    #     | "Separar la calificación de la descripcion" >> beam.Map(lambda l: l.split(","))
    #     | "Seleccionar únicamente los up y los down " >> beam.Map(lambda l: l[1])
    #     | "Conteo" >> beam.combiners.Count.PerElement()
    #     | "imprimir" >> beam.Map(print)
    #    )
    with beam.Pipeline(options=opts) as p:
        (p
         | "Leer entrada" >> beam.io.ReadFromText(entrada)
         | "Separar la calificación de la descripcion" >> beam.Map(lambda l: l.split(","))
         | "Numerar los valores en diccionarios" >> beam.Map(lambda l: (l[1] , 1))
         | "Contar por llave" >> beam.CombinePerKey(sum)
         | "Convertir a diccionario" >> beam.combiners.ToDict()
         | "Imprimir" >> beam.Map(print)
        )

if __name__ == "__main__":
    main()
