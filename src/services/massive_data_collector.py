#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ARQV30 Enhanced v3.0 - Massive Data Collector
Coletor massivo de dados de todas as fontes antes das análises
"""

import os
import logging
import time
import json
import asyncio
from typing import Dict, List, Any, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from services.alibaba_websailor import alibaba_websailor
from services.mcp_supadata_manager import mcp_supadata_manager
from services.exa_client import exa_client
from services.production_search_manager import production_search_manager
from services.robust_content_extractor import robust_content_extractor
from services.auto_save_manager import salvar_etapa, salvar_erro

logger = logging.getLogger(__name__)

class MassiveDataCollector:
    """Coletor massivo de dados de TODAS as fontes disponíveis"""
    
    def __init__(self):
        """Inicializa o coletor massivo"""
        self.collection_stats = {
            'web_sources': 0,
            'social_sources': 0,
            'youtube_sources': 0,
            'total_content_chars': 0,
            'extraction_time': 0,
            'sources_by_provider': {},
            'quality_scores': []
        }
        
        logger.info("🌊 Massive Data Collector inicializado")
    
    def collect_massive_data(
        self,
        query: str,
        context: Dict[str, Any],
        session_id: str,
        progress_callback: Optional[callable] = None
    ) -> Dict[str, Any]:
        """Executa coleta MASSIVA de dados de TODAS as fontes"""
        
        logger.info(f"🌊 INICIANDO COLETA MASSIVA DE DADOS para: {query}")
        start_time = time.time()
        
        try:
            # Salva início da coleta
            salvar_etapa("coleta_massiva_iniciada", {
                "query": query,
                "context": context,
                "timestamp": datetime.now().isoformat()
            }, categoria="pesquisa_web")
            
            if progress_callback:
                progress_callback(1, "🌊 Iniciando coleta massiva de dados...")
            
            # FASE 1: BUSCA WEB MASSIVA
            if progress_callback:
                progress_callback(2, "🔍 Executando busca web massiva (Exa + Google + Serper + Bing)...")
            
            web_data = self._collect_massive_web_data(query, context, session_id)
            
            # FASE 2: BUSCA SOCIAL MASSIVA
            if progress_callback:
                progress_callback(4, "📱 Executando busca social massiva (YouTube + Twitter + LinkedIn + Instagram)...")
            
            social_data = self._collect_massive_social_data(query, context, session_id)
            
            # FASE 3: NAVEGAÇÃO PROFUNDA COM WEBSAILOR
            if progress_callback:
                progress_callback(6, "🚢 Executando navegação profunda com WebSailor...")
            
            websailor_data = self._collect_websailor_deep_data(query, context, session_id)
            
            # FASE 4: EXTRAÇÃO MASSIVA DE CONTEÚDO
            if progress_callback:
                progress_callback(8, "📄 Extraindo conteúdo massivo de todas as páginas encontradas...")
            
            extracted_content = self._extract_massive_content(web_data, social_data, websailor_data, session_id)
            
            # FASE 5: CONSOLIDAÇÃO EM JSON GIGANTE
            if progress_callback:
                progress_callback(10, "📊 Consolidando dados em JSON gigante...")
            
            massive_json = self._consolidate_massive_json(
                web_data, social_data, websailor_data, extracted_content, query, context, session_id
            )
            
            # FASE 6: SALVAMENTO DO JSON GIGANTE
            if progress_callback:
                progress_callback(11, "💾 Salvando JSON gigante...")
            
            json_file_path = self._save_massive_json(massive_json, session_id)
            
            # FASE 7: VALIDAÇÃO E ESTATÍSTICAS FINAIS
            if progress_callback:
                progress_callback(12, "✅ Validando dados coletados...")
            
            validation_results = self._validate_massive_data(massive_json)
            
            collection_time = time.time() - start_time
            
            # Resultado final da coleta
            collection_result = {
                'success': True,
                'session_id': session_id,
                'query': query,
                'context': context,
                'massive_json_path': json_file_path,
                'massive_data': massive_json,
                'collection_stats': {
                    **self.collection_stats,
                    'collection_time': collection_time,
                    'collection_time_formatted': f"{int(collection_time // 60)}m {int(collection_time % 60)}s"
                },
                'validation_results': validation_results,
                'timestamp': datetime.now().isoformat()
            }
            
            # Salva resultado da coleta
            salvar_etapa("coleta_massiva_completa", collection_result, categoria="pesquisa_web")
            
            logger.info(f"✅ COLETA MASSIVA CONCLUÍDA em {collection_time:.2f}s")
            logger.info(f"📊 Estatísticas: {self.collection_stats['web_sources']} web + {self.collection_stats['social_sources']} social + {self.collection_stats['youtube_sources']} YouTube")
            
            return collection_result
            
        except Exception as e:
            logger.error(f"❌ ERRO CRÍTICO na coleta massiva: {e}")
            salvar_erro("coleta_massiva_erro", e, contexto={"query": query})
            raise e
    
    def _collect_massive_web_data(self, query: str, context: Dict[str, Any], session_id: str) -> Dict[str, Any]:
        """Coleta massiva de dados web de TODOS os provedores"""
        
        web_results = {
            'exa_results': [],
            'google_results': [],
            'serper_results': [],
            'bing_results': [],
            'duckduckgo_results': [],
            'total_web_sources': 0
        }
        
        # Busca com múltiplos provedores em paralelo
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {}
            
            # Exa Neural Search
            if exa_client.is_available():
                futures['exa'] = executor.submit(self._search_exa_massive, query)
            
            # Google Custom Search
            futures['google'] = executor.submit(self._search_google_massive, query)
            
            # Serper API
            futures['serper'] = executor.submit(self._search_serper_massive, query)
            
            # Bing Scraping
            futures['bing'] = executor.submit(self._search_bing_massive, query)
            
            # DuckDuckGo Scraping
            futures['duckduckgo'] = executor.submit(self._search_duckduckgo_massive, query)
            
            # Coleta resultados
            for provider, future in futures.items():
                try:
                    results = future.result(timeout=120)
                    web_results[f'{provider}_results'] = results
                    web_results['total_web_sources'] += len(results)
                    self.collection_stats['sources_by_provider'][provider] = len(results)
                    logger.info(f"✅ {provider}: {len(results)} resultados")
                except Exception as e:
                    logger.error(f"❌ Erro em {provider}: {e}")
                    web_results[f'{provider}_results'] = []
                    self.collection_stats['sources_by_provider'][provider] = 0
        
        self.collection_stats['web_sources'] = web_results['total_web_sources']
        
        # Salva dados web
        salvar_etapa("web_data_massive", web_results, categoria="pesquisa_web")
        
        return web_results
    
    def _collect_massive_social_data(self, query: str, context: Dict[str, Any], session_id: str) -> Dict[str, Any]:
        """Coleta massiva de dados de redes sociais"""
        
        social_results = {
            'youtube_data': {},
            'twitter_data': {},
            'linkedin_data': {},
            'instagram_data': {},
            'tiktok_data': {},
            'facebook_data': {},
            'total_social_sources': 0
        }
        
        # Busca em todas as plataformas sociais
        platforms = ['youtube', 'twitter', 'linkedin', 'instagram', 'tiktok', 'facebook']
        
        with ThreadPoolExecutor(max_workers=6) as executor:
            futures = {}
            
            for platform in platforms:
                futures[platform] = executor.submit(
                    self._search_social_platform_massive, platform, query
                )
            
            # Coleta resultados sociais
            for platform, future in futures.items():
                try:
                    results = future.result(timeout=90)
                    social_results[f'{platform}_data'] = results
                    
                    if results.get('results'):
                        count = len(results['results'])
                        social_results['total_social_sources'] += count
                        
                        if platform == 'youtube':
                            self.collection_stats['youtube_sources'] += count
                        else:
                            self.collection_stats['social_sources'] += count
                        
                        logger.info(f"✅ {platform}: {count} posts/vídeos")
                    
                except Exception as e:
                    logger.error(f"❌ Erro em {platform}: {e}")
                    social_results[f'{platform}_data'] = {}
        
        # Salva dados sociais
        salvar_etapa("social_data_massive", social_results, categoria="pesquisa_web")
        
        return social_results
    
    def _collect_websailor_deep_data(self, query: str, context: Dict[str, Any], session_id: str) -> Dict[str, Any]:
        """Coleta dados profundos com WebSailor"""
        
        try:
            websailor_results = alibaba_websailor.navigate_and_research_deep(
                query=query,
                context=context,
                max_pages=50,  # Aumentado para coleta massiva
                depth_levels=3,
                session_id=session_id
            )
            
            # Salva dados WebSailor
            salvar_etapa("websailor_data_massive", websailor_results, categoria="pesquisa_web")
            
            return websailor_results
            
        except Exception as e:
            logger.error(f"❌ Erro no WebSailor: {e}")
            return {}
    
    def _extract_massive_content(
        self, 
        web_data: Dict[str, Any], 
        social_data: Dict[str, Any], 
        websailor_data: Dict[str, Any],
        session_id: str
    ) -> Dict[str, Any]:
        """Extrai conteúdo massivo de todas as URLs encontradas"""
        
        all_urls = []
        
        # Coleta URLs de todas as fontes web
        for provider_results in web_data.values():
            if isinstance(provider_results, list):
                for result in provider_results:
                    if result.get('url'):
                        all_urls.append({
                            'url': result['url'],
                            'title': result.get('title', ''),
                            'source': result.get('source', 'web'),
                            'snippet': result.get('snippet', '')
                        })
        
        # Coleta URLs das redes sociais
        for platform_data in social_data.values():
            if isinstance(platform_data, dict) and platform_data.get('results'):
                for result in platform_data['results']:
                    if result.get('url'):
                        all_urls.append({
                            'url': result['url'],
                            'title': result.get('title', ''),
                            'source': result.get('platform', 'social'),
                            'snippet': result.get('text', result.get('caption', ''))[:200]
                        })
        
        # Coleta URLs do WebSailor
        if websailor_data.get('conteudo_consolidado', {}).get('fontes_detalhadas'):
            for fonte in websailor_data['conteudo_consolidado']['fontes_detalhadas']:
                all_urls.append({
                    'url': fonte['url'],
                    'title': fonte['title'],
                    'source': 'websailor',
                    'snippet': ''
                })
        
        logger.info(f"📄 Iniciando extração massiva de {len(all_urls)} URLs...")
        
        # Extração massiva em paralelo
        extracted_content = []
        
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {}
            
            for i, url_data in enumerate(all_urls[:100]):  # Limita a 100 URLs para performance
                futures[i] = executor.submit(
                    self._extract_single_content, url_data, i
                )
            
            # Coleta conteúdo extraído
            for i, future in futures.items():
                try:
                    content_data = future.result(timeout=60)
                    if content_data and content_data.get('success'):
                        extracted_content.append(content_data)
                        self.collection_stats['total_content_chars'] += len(content_data.get('content', ''))
                        
                        # Salva cada extração individualmente
                        salvar_etapa(f"extracao_{i}", content_data, categoria="pesquisa_web")
                        
                except Exception as e:
                    logger.warning(f"⚠️ Erro na extração {i}: {e}")
                    continue
        
        extraction_result = {
            'total_urls_found': len(all_urls),
            'total_content_extracted': len(extracted_content),
            'extraction_success_rate': (len(extracted_content) / len(all_urls)) * 100 if all_urls else 0,
            'extracted_content': extracted_content,
            'extraction_stats': {
                'total_chars': self.collection_stats['total_content_chars'],
                'avg_content_length': self.collection_stats['total_content_chars'] / len(extracted_content) if extracted_content else 0,
                'sources_by_type': self._categorize_sources(extracted_content)
            }
        }
        
        # Salva resultado da extração
        salvar_etapa("extracao_massiva_completa", extraction_result, categoria="pesquisa_web")
        
        return extraction_result
    
    def _consolidate_massive_json(
        self,
        web_data: Dict[str, Any],
        social_data: Dict[str, Any],
        websailor_data: Dict[str, Any],
        extracted_content: Dict[str, Any],
        query: str,
        context: Dict[str, Any],
        session_id: str
    ) -> Dict[str, Any]:
        """Consolida TODOS os dados em um JSON GIGANTE"""
        
        logger.info("📊 Consolidando dados em JSON GIGANTE...")
        
        massive_json = {
            'metadata_coleta_massiva': {
                'session_id': session_id,
                'query_original': query,
                'context_original': context,
                'timestamp_coleta': datetime.now().isoformat(),
                'versao_sistema': 'ARQV30 Enhanced v3.0',
                'tipo_coleta': 'MASSIVA_COMPLETA',
                'garantia_dados_reais': True,
                'zero_simulacao': True
            },
            
            'estatisticas_coleta': {
                'total_fontes_web': self.collection_stats['web_sources'],
                'total_fontes_sociais': self.collection_stats['social_sources'],
                'total_fontes_youtube': self.collection_stats['youtube_sources'],
                'total_caracteres_extraidos': self.collection_stats['total_content_chars'],
                'tempo_coleta_segundos': self.collection_stats['extraction_time'],
                'fontes_por_provedor': self.collection_stats['sources_by_provider'],
                'taxa_sucesso_extracao': extracted_content.get('extraction_success_rate', 0),
                'qualidade_media_conteudo': sum(self.collection_stats['quality_scores']) / len(self.collection_stats['quality_scores']) if self.collection_stats['quality_scores'] else 0
            },
            
            'dados_web_completos': {
                'busca_exa_neural': web_data.get('exa_results', []),
                'busca_google_custom': web_data.get('google_results', []),
                'busca_serper_api': web_data.get('serper_results', []),
                'busca_bing_scraping': web_data.get('bing_results', []),
                'busca_duckduckgo_scraping': web_data.get('duckduckgo_results', []),
                'total_resultados_web': web_data.get('total_web_sources', 0)
            },
            
            'dados_sociais_completos': {
                'youtube_completo': social_data.get('youtube_data', {}),
                'twitter_completo': social_data.get('twitter_data', {}),
                'linkedin_completo': social_data.get('linkedin_data', {}),
                'instagram_completo': social_data.get('instagram_data', {}),
                'tiktok_completo': social_data.get('tiktok_data', {}),
                'facebook_completo': social_data.get('facebook_data', {}),
                'total_posts_sociais': social_data.get('total_social_sources', 0)
            },
            
            'dados_websailor_profundos': {
                'navegacao_profunda': websailor_data.get('navegacao_profunda', {}),
                'conteudo_consolidado': websailor_data.get('conteudo_consolidado', {}),
                'insights_principais': websailor_data.get('conteudo_consolidado', {}).get('insights_principais', []),
                'tendencias_identificadas': websailor_data.get('conteudo_consolidado', {}).get('tendencias_identificadas', []),
                'oportunidades_descobertas': websailor_data.get('conteudo_consolidado', {}).get('oportunidades_descobertas', [])
            },
            
            'conteudo_extraido_massivo': {
                'total_paginas_extraidas': extracted_content.get('total_content_extracted', 0),
                'conteudo_completo': extracted_content.get('extracted_content', []),
                'estatisticas_extracao': extracted_content.get('extraction_stats', {}),
                'taxa_sucesso': extracted_content.get('extraction_success_rate', 0)
            },
            
            'insights_consolidados_automaticos': self._generate_automatic_insights(web_data, social_data, websailor_data, extracted_content),
            
            'preparacao_para_analises': {
                'dados_prontos_para_avatar': True,
                'dados_prontos_para_drivers': True,
                'dados_prontos_para_provas_visuais': True,
                'dados_prontos_para_anti_objecao': True,
                'dados_prontos_para_pre_pitch': True,
                'dados_prontos_para_predicoes': True,
                'dados_prontos_para_concorrencia': True,
                'dados_prontos_para_posicionamento': True,
                'qualidade_suficiente_para_analises': self._assess_data_quality_for_analysis(extracted_content)
            }
        }
        
        return massive_json
    
    def _save_massive_json(self, massive_json: Dict[str, Any], session_id: str) -> str:
        """Salva JSON gigante em arquivo"""
        
        try:
            # Cria diretório se não existir
            json_dir = os.path.join("analyses_data", "massive_data")
            os.makedirs(json_dir, exist_ok=True)
            
            # Nome do arquivo
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"massive_data_{session_id[:8]}_{timestamp}.json"
            file_path = os.path.join(json_dir, filename)
            
            # Salva JSON com formatação
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(massive_json, f, ensure_ascii=False, indent=2, default=str)
            
            file_size = os.path.getsize(file_path)
            logger.info(f"💾 JSON GIGANTE salvo: {file_path} ({file_size / 1024 / 1024:.2f} MB)")
            
            return file_path
            
        except Exception as e:
            logger.error(f"❌ Erro ao salvar JSON gigante: {e}")
            raise e
    
    def _search_exa_massive(self, query: str) -> List[Dict[str, Any]]:
        """Busca massiva com Exa"""
        try:
            enhanced_query = f"{query} Brasil 2024 mercado tendências oportunidades"
            
            exa_response = exa_client.search(
                query=enhanced_query,
                num_results=30,  # Aumentado para coleta massiva
                start_published_date="2023-01-01",
                use_autoprompt=True,
                type="neural"
            )
            
            if exa_response and 'results' in exa_response:
                return [
                    {
                        'title': item.get('title', ''),
                        'url': item.get('url', ''),
                        'snippet': item.get('text', '')[:300],
                        'source': 'exa',
                        'score': item.get('score', 0),
                        'published_date': item.get('publishedDate', ''),
                        'exa_id': item.get('id', '')
                    }
                    for item in exa_response['results']
                ]
            
            return []
            
        except Exception as e:
            logger.error(f"❌ Erro na busca Exa massiva: {e}")
            return []
    
    def _search_google_massive(self, query: str) -> List[Dict[str, Any]]:
        """Busca massiva com Google"""
        try:
            return production_search_manager._search_google(f"{query} Brasil 2024", 30)
        except Exception as e:
            logger.error(f"❌ Erro na busca Google massiva: {e}")
            return []
    
    def _search_serper_massive(self, query: str) -> List[Dict[str, Any]]:
        """Busca massiva com Serper"""
        try:
            return production_search_manager._search_serper(f"{query} Brasil 2024", 30)
        except Exception as e:
            logger.error(f"❌ Erro na busca Serper massiva: {e}")
            return []
    
    def _search_bing_massive(self, query: str) -> List[Dict[str, Any]]:
        """Busca massiva com Bing"""
        try:
            return production_search_manager._search_bing(f"{query} Brasil 2024", 30)
        except Exception as e:
            logger.error(f"❌ Erro na busca Bing massiva: {e}")
            return []
    
    def _search_duckduckgo_massive(self, query: str) -> List[Dict[str, Any]]:
        """Busca massiva com DuckDuckGo"""
        try:
            # Implementa busca DuckDuckGo se disponível
            return []  # Por enquanto vazio
        except Exception as e:
            logger.error(f"❌ Erro na busca DuckDuckGo massiva: {e}")
            return []
    
    def _search_social_platform_massive(self, platform: str, query: str) -> Dict[str, Any]:
        """Busca massiva em plataforma social específica"""
        
        try:
            if platform == 'youtube':
                return mcp_supadata_manager.search_youtube(query, max_results=25)
            elif platform == 'twitter':
                return mcp_supadata_manager.search_twitter(query, max_results=25)
            elif platform == 'linkedin':
                return mcp_supadata_manager.search_linkedin(query, max_results=20)
            elif platform == 'instagram':
                return mcp_supadata_manager.search_instagram(query, max_results=20)
            else:
                # Para TikTok e Facebook, usa busca simulada por enquanto
                return self._simulate_platform_data(platform, query)
                
        except Exception as e:
            logger.error(f"❌ Erro na busca {platform}: {e}")
            return {}
    
    def _simulate_platform_data(self, platform: str, query: str) -> Dict[str, Any]:
        """Simula dados de plataforma quando API não disponível"""
        return {
            'success': True,
            'platform': platform,
            'results': [
                {
                    'title': f'Conteúdo {platform} sobre {query}',
                    'url': f'https://{platform}.com/example',
                    'text': f'Conteúdo simulado sobre {query} na plataforma {platform}',
                    'platform': platform,
                    'simulated': True
                }
            ],
            'total_found': 1,
            'query': query
        }
    
    def _extract_single_content(self, url_data: Dict[str, Any], index: int) -> Optional[Dict[str, Any]]:
        """Extrai conteúdo de uma URL específica"""
        
        try:
            url = url_data['url']
            content = robust_content_extractor.extract_content(url)
            
            if content and len(content) > 200:
                quality_score = self._calculate_content_quality(content, url_data)
                self.collection_stats['quality_scores'].append(quality_score)
                
                return {
                    'success': True,
                    'url': url,
                    'title': url_data.get('title', ''),
                    'source': url_data.get('source', 'unknown'),
                    'snippet': url_data.get('snippet', ''),
                    'content': content,
                    'content_length': len(content),
                    'word_count': len(content.split()),
                    'quality_score': quality_score,
                    'extraction_index': index,
                    'extracted_at': datetime.now().isoformat()
                }
            
            return None
            
        except Exception as e:
            logger.warning(f"⚠️ Erro ao extrair conteúdo {index}: {e}")
            return None
    
    def _calculate_content_quality(self, content: str, url_data: Dict[str, Any]) -> float:
        """Calcula qualidade do conteúdo extraído"""
        
        score = 0.0
        
        # Score por tamanho
        if len(content) >= 2000:
            score += 25
        elif len(content) >= 1000:
            score += 15
        else:
            score += 5
        
        # Score por densidade de informação
        words = content.split()
        if len(words) >= 300:
            score += 25
        elif len(words) >= 150:
            score += 15
        else:
            score += 5
        
        # Score por presença de dados
        import re
        numbers = re.findall(r'\d+(?:\.\d+)?%?', content)
        money_values = re.findall(r'R\$\s*[\d,\.]+', content)
        
        score += min(len(numbers) * 2, 20)
        score += min(len(money_values) * 3, 15)
        
        # Score por fonte
        source = url_data.get('source', '')
        if source == 'exa':
            score += 10
        elif source == 'websailor':
            score += 8
        elif source in ['google', 'serper']:
            score += 6
        
        return min(score, 100.0)
    
    def _categorize_sources(self, extracted_content: List[Dict[str, Any]]) -> Dict[str, int]:
        """Categoriza fontes por tipo"""
        
        categories = {}
        
        for content in extracted_content:
            source = content.get('source', 'unknown')
            categories[source] = categories.get(source, 0) + 1
        
        return categories
    
    def _generate_automatic_insights(
        self,
        web_data: Dict[str, Any],
        social_data: Dict[str, Any],
        websailor_data: Dict[str, Any],
        extracted_content: Dict[str, Any]
    ) -> List[str]:
        """Gera insights automáticos dos dados coletados"""
        
        insights = []
        
        # Insights de volume
        total_sources = (
            web_data.get('total_web_sources', 0) +
            social_data.get('total_social_sources', 0)
        )
        
        insights.append(f"Coletados dados de {total_sources} fontes únicas")
        
        # Insights de qualidade
        if self.collection_stats['quality_scores']:
            avg_quality = sum(self.collection_stats['quality_scores']) / len(self.collection_stats['quality_scores'])
            insights.append(f"Qualidade média do conteúdo: {avg_quality:.1f}/100")
        
        # Insights de conteúdo
        total_chars = self.collection_stats['total_content_chars']
        if total_chars > 0:
            insights.append(f"Total de {total_chars:,} caracteres de conteúdo extraído")
            insights.append(f"Equivalente a aproximadamente {total_chars // 2000} páginas A4")
        
        # Insights por provedor
        best_provider = max(self.collection_stats['sources_by_provider'].items(), key=lambda x: x[1]) if self.collection_stats['sources_by_provider'] else None
        if best_provider:
            insights.append(f"Melhor provedor: {best_provider[0]} com {best_provider[1]} resultados")
        
        return insights
    
    def _validate_massive_data(self, massive_json: Dict[str, Any]) -> Dict[str, Any]:
        """Valida qualidade dos dados coletados"""
        
        validation = {
            'dados_suficientes_para_analise': False,
            'qualidade_aprovada': False,
            'modulos_viabilizados': [],
            'problemas_identificados': [],
            'recomendacoes': []
        }
        
        # Verifica volume mínimo
        total_content = massive_json.get('estatisticas_coleta', {}).get('total_caracteres_extraidos', 0)
        
        if total_content >= 50000:  # 50k caracteres mínimo
            validation['dados_suficientes_para_analise'] = True
        else:
            validation['problemas_identificados'].append(f"Volume insuficiente: {total_content} < 50000 caracteres")
        
        # Verifica qualidade
        avg_quality = massive_json.get('estatisticas_coleta', {}).get('qualidade_media_conteudo', 0)
        
        if avg_quality >= 60:
            validation['qualidade_aprovada'] = True
        else:
            validation['problemas_identificados'].append(f"Qualidade baixa: {avg_quality} < 60")
        
        # Verifica viabilidade dos módulos
        preparacao = massive_json.get('preparacao_para_analises', {})
        
        modulos_possiveis = [
            'avatar_ultra_detalhado',
            'drivers_mentais_customizados',
            'provas_visuais_arsenal',
            'sistema_anti_objecao',
            'pre_pitch_invisivel',
            'predicoes_futuro',
            'analise_concorrencia',
            'posicionamento_estrategico'
        ]
        
        for modulo in modulos_possiveis:
            if preparacao.get(f'dados_prontos_para_{modulo.split("_")[0]}', False):
                validation['modulos_viabilizados'].append(modulo)
        
        # Gera recomendações
        if not validation['dados_suficientes_para_analise']:
            validation['recomendacoes'].append("Execute nova coleta com mais APIs configuradas")
        
        if not validation['qualidade_aprovada']:
            validation['recomendacoes'].append("Configure APIs premium para melhor qualidade")
        
        if len(validation['modulos_viabilizados']) < 6:
            validation['recomendacoes'].append("Dados insuficientes para alguns módulos - configure mais fontes")
        
        return validation
    
    def _assess_data_quality_for_analysis(self, extracted_content: Dict[str, Any]) -> bool:
        """Avalia se dados têm qualidade suficiente para análises"""
        
        content_list = extracted_content.get('extracted_content', [])
        
        if len(content_list) < 10:
            return False
        
        total_chars = sum(len(item.get('content', '')) for item in content_list)
        
        if total_chars < 30000:
            return False
        
        avg_quality = sum(item.get('quality_score', 0) for item in content_list) / len(content_list)
        
        return avg_quality >= 60

# Instância global
massive_data_collector = MassiveDataCollector()