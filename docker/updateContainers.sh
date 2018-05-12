#!/bin/bash

docker build -t $1/base_recal:dev docker/base_recal
docker build -t $1/base_recal_table:dev docker/base_recal_table
docker build -t $1/bwa_mem:dev docker/bwa_mem
docker build -t $1/index_bam:dev docker/index_bam
docker build -t $1/mark_dups:dev docker/mark_dups
docker build -t $1/sort_sam:dev docker/sort_sam
docker build -t $1/vqsr_indel_apply:dev docker/vqsr_indel_apply
docker build -t $1/vqsr_indel_model:dev docker/vqsr_indel_model
docker build -t $1/vqsr_snp_apply:dev docker/vqsr_snp_apply
docker build -t $1/vqsr_snp_model:dev docker/vqsr_snp_model
docker build -t $1/sentieon_haplotyper:dev docker/sentieon_haplotyper
docker build -t $1/sentieon_genotyper:dev docker/sentieon_genotyper

docker push $1/base_recal:dev
docker push $1/base_recal_table:dev
docker push $1/bwa_mem:dev
docker push $1/index_bam:dev
docker push $1/mark_dups:dev
docker push $1/sort_sam:dev
docker push $1/vqsr_indel_apply:dev
docker push $1/vqsr_indel_model:dev
docker push $1/vqsr_snp_apply:dev
docker push $1/vqsr_snp_model:dev
docker push $1/sentieon_haplotyper:dev
docker push $1/sentieon_genotyper:dev
